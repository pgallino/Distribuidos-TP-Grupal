import logging
import signal
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware


class Replica:
    def __init__(self, id: int, n_instances: int):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.replica_ids = list(range(1, n_instances + 1))
        # Inicialización específica
        self.listener_process = None
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

        if self.listener_process:
            self.listener_process.terminate()
            self.listener_process.join()

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
            # logging.info(f"Recibi un mensaje del tipo: {msg.type}")

            if msg.type == MsgType.PUSH_DATA:
                # Procesar datos replicados
                # logging.info("Replica: procesando datos de `push`.")
                self._process_push_data(msg)

            elif msg.type == MsgType.PULL_DATA:
                logging.info("Voy a mandar la data")
                # Responder con toda la data replicada
                self._send_data()

        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)
