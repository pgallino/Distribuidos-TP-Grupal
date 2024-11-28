import logging
import signal
import socket
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from election.election_manager import ElectionManager
from utils.constants import E_FROM_MASTER_PUSH, Q_MASTER_REPLICA, Q_REPLICA_MASTER
from utils.utils import reanimate_container, recv_msg

PORT_ENTRE_REPLICAS = 8080
TIMEOUT = 5

class Replica:
    def __init__(self, id: int, n_instances: int, ip_prefix: str, port: int, container_to_restart: str, timeout: int):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.replica_ids = list(range(1, n_instances + 1))
        self.container_to_restart = container_to_restart
        self.state = None
        self.ip_prefix = ip_prefix
        self.port = port

        self.timeout = timeout
        self._initialize_storage()

        if not ip_prefix:
            raise ValueError("ip_prefix no puede ser None o vacío.")
        if not container_to_restart:
            raise ValueError("container_to_restart no puede ser None o vacío.")


        self.recv_queue = Q_MASTER_REPLICA + f"_{ip_prefix}_{self.id}"
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
            ip_prefix=ip_prefix,
            port=port
        )

        # Determinar si esta réplica es el líder inicial
        self.leader_id = max(self.replica_ids)  # El mayor ID es el líder inicial
        if self.id == self.leader_id:
            logging.info(f"Replica {self.id}: Soy el líder inicial.")

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""

        try:
            while True:
                self._middleware.receive_from_queue_with_timeout(self.recv_queue, self.process_replica_message, self.timeout, auto_ack=False)
                self.ask_keepalive()
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: shutdown_replica | result: fail | error: {e}")
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
            self.send_queue, # Cola de respuesta
            response_data.encode()
        )

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
                # TODO: Mandar de a batches?
                if self.leader_id == self.id:
                    self._process_pull_data()

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")

    def ask_keepalive(self):
        """Verifica si el nodo maestro está vivo."""
        sock = None
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            logging.info("Intento conectarme al nodo maestro...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.container_to_restart, self.port))
            logging.info("Conexión exitosa. MASTER está vivo.")
        except socket.gaierror as e:
            logging.info("DETECCIÓN DE MASTER INACTIVO: Nodo maestro no encontrado.")
            self.handle_master_down()
        except socket.timeout:
            logging.info("DETECCIÓN DE MASTER INACTIVO: Tiempo de espera agotado.")
            self.handle_master_down()
        except OSError as e:
            logging.info("DETECCIÓN DE MASTER INACTIVO: Nodo maestro inalcanzable.")
            self.handle_master_down()
        except Exception as e:
            logging.error(f"Error inesperado durante la conexión: {e}")
            if not self.shutting_down:
                logging.error(f"Error inesperado en run: {e}")
        finally:
            if sock:
                sock.close()
                logging.info("Socket cerrado correctamente.")

    def handle_master_down(self):
        self.leader_id = self.election_manager.manage_leadership()
        logging.info(f"EL LEADER SELECCIONADO RETORNADO: {self.leader_id}")
        if self.leader_id == self.id:
            logging.info(f"Replica {self.id}: Soy el líder, ejecutando reanimate_container...")
            reanimate_container(self.container_to_restart)  # Ejecuta la función como líder
            self.notify_replicas()  # Notifica a los otros nodos
        else:
            self.wait_for_leader()

    def notify_replicas(self):
        """Envía un mensaje al resto de las réplicas notificando que se ha reanimado el contenedor."""
        for replica_id in self.replica_ids:
            if replica_id != self.id:  # No enviar a sí mismo
                try:
                    logging.info(f"Replica {self.id}: Notificando a réplica {replica_id}...")
                    with socket.create_connection((f"{self.ip_prefix}_{replica_id}", PORT_ENTRE_REPLICAS), timeout=5) as sock:
                        msg = SimpleMessage(type=MsgType.MASTER_REANIMATED, socket_compatible=True)
                        sock.sendall(msg.encode())
                        logging.info(f"Replica {self.id}: Notificación enviada a réplica {replica_id}.")
                except socket.timeout:
                    logging.error(f"Replica {self.id}: Timeout al notificar a réplica {replica_id}.")
                except Exception as e:
                    logging.error(f"Error al notificar a réplica {replica_id}: {e}")

    def wait_for_leader(self):
        """Espera a que el líder notifique que se ha ejecutado reanimate_container."""
        logging.info(f"Replica {self.id}: Esperando notificación del líder...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((f"{self.ip_prefix}_{self.id}", PORT_ENTRE_REPLICAS))
            server_socket.listen(1)  # Solo se espera una conexión
            conn, _ = server_socket.accept()
            raw_msg = recv_msg(conn)
            msg = decode_msg(raw_msg)
            if msg.type == MsgType.MASTER_REANIMATED:
                logging.info(f"Replica {self.id}: El lider me notifico que ya reanimo al master, continuo...")

