import logging
import signal
import socket
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
from election.election_manager import ElectionManager
from utils.constants import E_FROM_MASTER_PUSH, Q_MASTER_REPLICA, Q_REPLICA_MASTER
from utils.utils import reanimate_container

PORT = 12345
TIMEOUT = 5

class Replica:
    def __init__(self, id: int, n_instances: int, container_name: str, container_to_restart: str, timeout: int):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.replica_ids = list(range(1, n_instances + 1))
        self.container_name = container_name
        self.container_to_restart = container_to_restart

        self.timeout = timeout
        self._initialize_storage()

        if not container_name:
            raise ValueError("container_name no puede ser None o vacío.")
        if not container_to_restart:
            raise ValueError("container_to_restart no puede ser None o vacío.")


        self.recv_queue = Q_MASTER_REPLICA + f"_{container_name}_{self.id}"
        self.send_queue = Q_REPLICA_MASTER + f"_{container_to_restart}"
        exchange_name = E_FROM_MASTER_PUSH + f"_{container_to_restart}"
        self._middleware.declare_queue(self.recv_queue) # -> cola por donde recibo pull y push
        self._middleware.declare_exchange(exchange_name, type = "fanout")
        self._middleware.bind_queue(self.recv_queue, exchange_name) # -> bindeo al fanout de los push y pull
        self._middleware.declare_queue(self.send_queue) # -> cola para enviar

        # Inicializa el ElectionManager
        self.election_manager = ElectionManager(
            id=self.id,
            ids=self.replica_ids,
            container_name=container_name,
            on_leader_selected=reanimate_container,
            container_to_restart=container_to_restart
        )

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""

        while True:
            self._middleware.receive_from_queue_with_timeout(self.recv_queue, self.process_replica_message, self.timeout, auto_ack=False)
            self.ask_keepalive()

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""
        pass

    def _process_push_data(self, msg):
        """Procesa los datos de un mensaje de `PushDataMessage`."""
        pass

    def _process_pull_data(self):
        """Codifica el estado actual en un formato que pueda ser enviado en una respuesta."""
        pass

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return

        logging.info("action: shutdown_replica | result: in progress...")
        self.shutting_down = True

        self.election_manager.cleanup()

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
                # Responder con toda la data replicada
                self._process_pull_data()

        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def ask_keepalive(self):
        """Verifica si el nodo maestro está vivo."""
        sock = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            logging.info("Intento conectarme al nodo maestro...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.container_to_restart, PORT))
            logging.info("Conexión exitosa. MASTER está vivo.")
        except socket.gaierror as e:
            logging.error(f"Error en resolución de nombre del nodo maestro: {e}")
            logging.info("DETECCIÓN DE MASTER INACTIVO: Nodo maestro no encontrado.")
            self.election_manager.manage_leadership()
        except socket.timeout:
            logging.error("Timeout al intentar conectarse al nodo maestro.")
            logging.info("DETECCIÓN DE MASTER INACTIVO: Tiempo de espera agotado.")
            self.election_manager.manage_leadership()
        except OSError as e:
            logging.error(f"Error de sistema al intentar conectarse: {e}")
            logging.info("DETECCIÓN DE MASTER INACTIVO: Nodo maestro inalcanzable.")
            self.election_manager.manage_leadership()
        except Exception as e:
            logging.error(f"Error inesperado durante la conexión: {e}")
            if not self.shutting_down:
                logging.error(f"Error inesperado en run: {e}")
        finally:
            if sock:
                sock.close()
                logging.info("Socket cerrado correctamente.")


