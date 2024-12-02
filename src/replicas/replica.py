import logging
from multiprocessing import Process, Manager
import signal
import socket
import time
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from election.election_manager import ElectionManager
from utils.middleware_constants import E_FROM_MASTER_PUSH, Q_MASTER_REPLICA, Q_REPLICA_MASTER
from utils.container_constants import ELECTION_PORT, LISTENER_PORT
from utils.listener import ReplicaListener
from utils.utils import TaskType, recv_msg

# Diseño: https://frill-bucket-81f.notion.site/Dise-o-Replicas-14d5c77282b880cca2d6f11f42c4d2de?pvs=4

TIMEOUT = 5

class Replica:
    def __init__(self, id: int, n_instances: int, ip_prefix: str, container_to_restart: str, timeout: int):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.replica_ids = list(range(1, n_instances + 1))
        self.container_to_restart = container_to_restart
        self.state = None
        self.ip_prefix = ip_prefix
        self.port = LISTENER_PORT

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.timeout = timeout
        self._initialize_storage()

        if not ip_prefix:
            raise ValueError("ip_prefix no puede ser None o vacío.")
        if not container_to_restart:
            raise ValueError("container_to_restart no puede ser None o vacío.")

        # self.recv_queue = Q_MASTER_REPLICA + f"_{ip_prefix}_{self.id}"
        self.send_queue = Q_REPLICA_MASTER + f"_{container_to_restart}"
        exchange_name = E_FROM_MASTER_PUSH + f"_{container_to_restart}"
        self._middleware.declare_exchange(exchange_name, type = "fanout")
        # self._middleware.bind_queue(self.recv_queue, exchange_name) # -> bindeo al fanout de los push y pull
        self._middleware.declare_queue(self.send_queue) # -> cola para enviar
        self.recv_queue = self._middleware.declare_anonymous_queue(exchange_name=exchange_name)
        
        # Inicializa el ElectionManager
        self.election_manager = ElectionManager(
            id=self.id,
            ids=self.replica_ids,
            ip_prefix=ip_prefix,
            port=ELECTION_PORT
        )

        logging.info("espere a que todos se levantaran")

        # Crear las variables compartidas
        self.manager = Manager()
        self.task_status = self.manager.dict({"intent": None, "completed": None})
        self.task_condition = self.manager.Condition()
        self.leader_id = self.manager.Value("i", 0) # 0 indica que no hay lider
        
        task_coordination_vars = (self.task_status, self.task_condition)
        self.listener = Process(target=init_listener, args=(id, self.replica_ids, ip_prefix, self.port, task_coordination_vars, self.leader_id))
        self.listener.start()

        time.sleep(10)
        # En el constructor (__init__) o método de inicialización de Replica
        self.leader_id.value = self.check_for_leader_or_elect()
        logging.info(f"Replica {self.id}: Mi líder actual es {self.leader_id.value}.")

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        try:
            while not self.shutting_down:
                # AHORA REVIVE EL WATCHDOG A LOS MASTERS, NO NECESITO VERIFICAR CON TIMEOUT
                self._middleware.receive_from_queue(self.recv_queue, self.process_replica_message, auto_ack=False)
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

    def check_for_leader_or_elect(self):
        """
        Consulta a las réplicas si hay un líder activo. Si no hay, inicia una elección y devuelve el ID del líder.
        """
        logging.info(f"Replica {self.id}: Verificando si existe un líder activo...")

        for replica_id in self.replica_ids:
            if replica_id == self.id:
                continue  # No me consulto a mí mismo

            target_ip = f"{self.ip_prefix}_{replica_id}"
            try:
                with socket.create_connection((target_ip, self.port), timeout=self.timeout) as sock:
                    # Enviar consulta sobre el líder
                    msg = SimpleMessage(type=MsgType.WHO_IS_LEADER, socket_compatible=True)
                    sock.sendall(msg.encode())

                    # Recibir la respuesta
                    raw_response = recv_msg(sock)
                    response = decode_msg(raw_response)

                    # Validar que la respuesta sea del tipo esperado y que el líder sea válido (no 0)
                    if response.type == MsgType.CURRENT_LEADER and response.current_leader != 0:
                        logging.info(f"Replica {self.id}: Líder detectado: {response.current_leader}.")
                        return response.current_leader
                    elif response.current_leader == 0:
                        logging.warning(f"Replica {self.id}: Réplica {replica_id} indicó que no hay líder.")
                    else:
                        logging.warning(f"Replica {self.id}: Respuesta inesperada de {replica_id}: {response.type}.")
            except socket.timeout:
                logging.warning(f"Replica {self.id}: Timeout al intentar conectar con réplica {replica_id}.")
            except (socket.error, OSError) as e:
                logging.warning(f"Replica {self.id}: Error de conexión con réplica {replica_id}: {e}")
            except Exception as e:
                logging.error(f"Replica {self.id}: Error inesperado al consultar réplica {replica_id}: {e}")

        # Si no se detectó un líder válido, iniciar elección
        logging.info(f"Replica {self.id}: No se detectó líder activo. Iniciando elección.")
        return self.election_manager.manage_leadership()

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
        self.manager.shutdown()

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

                if self.leader_id.value == self.id:
                    self.handle_pull()
                else:
                    # Si no soy el líder, esperar a que lleguen los mensajes de INTENT y COMPLETED
                    self.wait_for_task(task_type=TaskType.PULL)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")

    def ask_keepalive_leader(self):
        leader_ip = f"{self.ip_prefix}_{self.leader_id}"
        if not self.check_node_status(leader_ip, self.port):
            logging.info("Leader no disponible, iniciando elección.")
            self.leader_id = self.election_manager.manage_leadership()
            logging.info(f"NUEVO LEADER RECIBIDO: {self.leader_id}")
        else:
            logging.info("Leader activo y accesible.")

    def check_node_status(self, target_ip, target_port):
        try:
            with socket.create_connection((target_ip, target_port), timeout=self.timeout) as sock:
                logging.info(f"Conexión exitosa a {target_ip}:{target_port}.")
                msg = SimpleMessage(type=MsgType.KEEP_ALIVE, socket_compatible=True)
                sock.sendall(msg.encode())
                return True
        except (socket.gaierror, socket.timeout, OSError) as e:
            logging.info(f"Error conectando a {target_ip}:{target_port}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error inesperado conectando a {target_ip}:{target_port}: {e}")
            return False
        
    def simulate_failure(self, id):
        """Simula la caída del id"""

        if self.id == id:
            self._shutdown()
            exit(0)

    # TODO: Se puede hacer una sola funcion
    def send_task_intent(self, task_type):
        """Envía un mensaje TASK_INTENT a las réplicas."""
        intent_msg = SimpleMessage(type=MsgType.TASK_INTENT, socket_compatible=True, node_id=self.id, task_type=task_type.value)
        self.broadcast_to_replicas(intent_msg)
        logging.info(f"Replica {self.id}: TASK_INTENT enviado para tarea '{task_type}'.")

    def send_task_completed(self, task_type):
        """Envía un mensaje TASK_COMPLETED a las réplicas."""
        completed_msg = SimpleMessage(type=MsgType.TASK_COMPLETED, socket_compatible=True, node_id=self.id, task_type=task_type.value)
        self.broadcast_to_replicas(completed_msg)
        logging.info(f"Replica {self.id}: TASK_COMPLETED enviado para tarea '{task_type}'.")

    def broadcast_to_replicas(self, message):
        for replica_id in self.replica_ids:
            if replica_id != self.id:  # No enviar a sí mismo
                for _ in range(3):  # Reintentar 3 veces
                    try:
                        target_ip = f"{self.ip_prefix}_{replica_id}"
                        with socket.create_connection((target_ip, self.port), timeout=1) as sock:
                            sock.sendall(message.encode())
                            logging.info(f"Replica {self.id}: Mensaje enviado a réplica {replica_id}.")
                            break
                    except Exception as e:
                        logging.warning(f"Replica {self.id}: Reintento fallido para réplica {replica_id}: {e}")

    def wait_for_task(self, task_type: TaskType):
        """
        Espera a que el líder envíe TASK_INTENT y TASK_COMPLETED para una tarea específica.
        Si el tiempo de espera excede el límite, se verifica el estado del líder.
        """
        timeout_seconds = 5  # Tiempo máximo de espera antes de verificar keep_alive
        with self.task_condition:
            while self.task_status["intent"] != task_type.value or self.task_status["completed"] != task_type.value:
                logging.info(f"Replica {self.id}: Esperando TASK_INTENT y TASK_COMPLETED para '{task_type.name}'...")

                # Esperar con timeout
                leader_responded = self.task_condition.wait(timeout=timeout_seconds)

                if not leader_responded:  # Si se cumple el timeout
                    logging.warning(f"Replica {self.id}: Timeout esperando TASK_INTENT y TASK_COMPLETED para '{task_type.name}'. Verificando líder.")
                    
                    # Verificar si el líder sigue activo
                    leader_ip = f"{self.ip_prefix}_{self.leader_id.value}"
                    if not self.check_node_status(leader_ip, self.port):
                        logging.warning(f"Replica {self.id}: El líder no responde. Iniciando nueva elección.")
                        self.leader_id.value = self.election_manager.manage_leadership()
                        logging.info(f"NUEVO LEADER RECIBIDO: {self.leader_id.value}")

                        if self.leader_id.value == self.id:
                            logging.info(f"Replica {self.id}: Soy el nuevo líder tras la elección. Procesando tarea '{task_type.name}'.")
                            # Procesar la tarea como nuevo líder
                            if task_type == TaskType.PULL:
                                self.handle_pull()
                                return
                    else:
                        logging.info(f"Replica {self.id}: El líder sigue activo. Continuando espera.")

        # Limpieza de los valores de la tarea después de recibir ambos eventos
        self.task_status["intent"] = None
        self.task_status["completed"] = None
        logging.info(f"Replica {self.id}: TASK_INTENT y TASK_COMPLETED procesados y limpiados para '{task_type.name}'.")

    def handle_pull(self):
        """
        Maneja el proceso de PULL como líder.
        Envía TASK_INTENT, realiza la sincronización y luego envía TASK_COMPLETED.
        """
        # Verificar si el maestro está sincronizado
        if not self.ask_master_connected():
            logging.info(f"Replica {self.id}: Iniciando proceso de PULL como líder.")

            # 1. Enviar TASK_INTENT a las otras réplicas
            self.send_task_intent(task_type=TaskType.PULL)
            # 2. Procesar el PULL (sincronización con el maestro)
            self._process_pull_data()

            # 3. Enviar TASK_COMPLETED a las otras réplicas
            self.send_task_completed(task_type=TaskType.PULL)

            logging.info(f"Replica {self.id}: Proceso de PULL completado y notificado a réplicas.")
        else:
            self.send_task_completed(task_type=TaskType.PULL)
            logging.info(f"Replica {self.id}: Maestro ya sincronizado. No se necesita PULL.")

    def ask_master_connected(self):
        """Consulta al maestro si está sincronizado."""
        try:
            master_ip = self.container_to_restart  # Dirección IP del maestro
            with socket.create_connection((master_ip, self.port), timeout=self.timeout) as sock:
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
        except Exception as e:
            logging.error(f"Replica {self.id}: Error consultando maestro: {e}")
            # TODO: pasa que aun no se le levantó el listener y no llega a responder despues de una reanimacion
            # TODO: habria que distinguir el caso en que no me respondio porque aun no se le levanto el listener post reanimacion o de si murió post reanimacion instantaneamente.
            return False  # Por defecto, asumir que no está conectado


def init_listener(id, replica_ids, ip_prefix, port, master_coordination_vars, leader_id):
    listener = ReplicaListener(id, replica_ids, ip_prefix, port, master_coordination_vars, leader_id)
    listener.run()