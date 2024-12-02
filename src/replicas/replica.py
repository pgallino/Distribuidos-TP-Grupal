import logging
from multiprocessing import Process
import signal
import socket
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.middleware_constants import E_FROM_MASTER_PUSH, E_FROM_REPLICA_PULL, Q_MASTER_REPLICA, Q_REPLICA_MASTER
from utils.container_constants import LISTENER_PORT
from utils.listener import ReplicaListener
from utils.utils import TaskType, recv_msg

class Replica:
    def __init__(self, id: int, ip_prefix: str, container_to_restart: str):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.container_to_restart = container_to_restart
        self.state = None
        self.ip_prefix = ip_prefix
        self.port = LISTENER_PORT

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self._initialize_storage()

        if not ip_prefix:
            raise ValueError("ip_prefix no puede ser None o vacío.")
        if not container_to_restart:
            raise ValueError("container_to_restart no puede ser None o vacío.")

        self.recv_queue = Q_MASTER_REPLICA + f"_{ip_prefix}_{self.id}"
        exchange_name = E_FROM_MASTER_PUSH + f"_{container_to_restart}"
        self._middleware.declare_queue(self.recv_queue) # -> cola por donde recibo pull y push
        self._middleware.declare_exchange(exchange_name, type = "fanout")
        self._middleware.bind_queue(self.recv_queue, exchange_name) # -> bindeo al fanout de los push y pull
        self._middleware.declare_exchange(E_FROM_REPLICA_PULL)
        
        self.listener = Process(target=init_listener, args=(id, ip_prefix, self.port,))
        self.listener.start()

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        try:
            while not self.shutting_down:
                # AHORA REVIVE EL WATCHDOG A LOS MASTERS, NO NECESITO VERIFICAR CON TIMEOUT
                self._middleware.receive_from_queue(self.recv_queue, self.process_replica_message, auto_ack=False)
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listening_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""
        pass

    def _process_push_data(self, msg):
        """Procesa los datos de un mensaje de `PushDataMessage`."""
        pass

    def _process_pull_data(self):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""

        # Crear el mensaje de respuesta con el estado actual

        logging.info("Respondiendo solicitud de pull por master")
        response_data = PushDataMessage( data=dict(self.state))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            E_FROM_REPLICA_PULL, # Cola de respuesta
            response_data.encode()
        )

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return

        logging.info("action: shutdown_replica | result: in progress...")
        self.shutting_down = True

        if self.listener:
            self.listener.terminate()
            self.listener.join()

        try:
            self._middleware.close()
            logging.info("action: shutdown_replica | result: success")
        except Exception as e:
            logging.error(f"action: shutdown_replica | result: fail | error: {e}")

        self._middleware.check_closed()
        exit(0)

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
                self._process_push_data(msg)

            elif msg.type == MsgType.PULL_DATA:
                self.handle_pull()

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")
        
    def simulate_failure(self, id):
        """Simula la caída del id"""

        if self.id == id:
            self._shutdown()
            exit(0)

    def handle_pull(self):
        """
        Maneja el proceso de PULL como líder.
        Envía TASK_INTENT, realiza la sincronización y luego envía TASK_COMPLETED.
        """
        # Verificar si el maestro está sincronizado
        if not self.ask_master_connected():
            self._process_pull_data()

    def ask_master_connected(self):
        """
        Consulta al maestro si está sincronizado.
        Si no responde a tiempo (timeout), asume que no está conectado.
        """
        try:
            master_ip = self.container_to_restart  # Dirección IP del maestro
            with socket.create_connection((master_ip, self.port), timeout=3) as sock:
                # Enviar el mensaje ASK_MASTER_CONNECTED
                ask_msg = SimpleMessage(type=MsgType.ASK_MASTER_CONNECTED, socket_compatible=True)
                sock.sendall(ask_msg.encode())

                # Recibir la respuesta
                raw_response = recv_msg(sock)
                response_msg = decode_msg(raw_response)

            if response_msg.type == MsgType.MASTER_CONNECTED:
                is_connected = bool(response_msg.connected)  # Convertir 0/1 a booleano
                logging.info(f"Recibido MASTER_CONNECTED: connected={is_connected}")
                return is_connected
        except socket.timeout:
            # Caso específico: timeout en la conexión o respuesta
            logging.warning(f"Replica {self.id}: Timeout esperando respuesta del maestro.")
            return False
        except Exception as e:
            # Cualquier otro error se captura aquí
            logging.error(f"Replica {self.id}: Error consultando maestro: {e}")
            return False

        # Si no se recibió ningún mensaje válido, asumimos desconexión
        return False

def init_listener(id, ip_prefix, port):
    listener = ReplicaListener(id, ip_prefix, port)
    listener.run()