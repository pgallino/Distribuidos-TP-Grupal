from collections import defaultdict
import logging
import signal
from messages.messages import MsgType, PushDataMessage, decode_msg
from middleware.middleware import Middleware
from utils.constants import Q_REPLICA, E_REPLICA


class Replica:
    def __init__(self, id: int):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_REPLICA)
        self._middleware.declare_exchange(E_REPLICA)
        self._middleware.bind_queue(Q_REPLICA, E_REPLICA, "replica_data")

        # Inicialización específica
        self._initialize_storage()

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""
        pass

    def _process_push_data(self, msg):
        """Procesa los datos de un mensaje de `PushDataMessage`."""
        pass

    def _send_data(self):
        """Codifica el estado actual en un formato que pueda ser enviado en una respuesta."""
        pass

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return

        logging.info("action: shutdown_replica | result: in progress...")
        self.shutting_down = True

        try:
            self._middleware.close()
            logging.info("action: shutdown_replica | result: success")
        except Exception as e:
            logging.error(f"action: shutdown_replica | result: fail | error: {e}")

        self._middleware.check_closed()

    def _handle_sigterm(self, sig, frame):
        """Maneja la señal SIGTERM para cerrar la réplica de forma segura."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()

    def process_replica_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_REPLICA`."""
        try:
            msg = decode_msg(raw_message)

            if msg.type == MsgType.PUSH_DATA:
                # Procesar datos replicados
                logging.info("Replica: procesando datos de `push`.")
                self._process_push_data(msg)

            elif msg.type == MsgType.PULL_DATA:
                # Responder con toda la data replicada
                logging.info("Replica: respondiendo a solicitud de `pull`.")
                self._send_data(msg)

        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        self._middleware.receive_from_queue(Q_REPLICA, self.process_replica_message)