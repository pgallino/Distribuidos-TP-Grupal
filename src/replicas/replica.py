import logging
from multiprocessing import Condition, Value, Process
import signal
import socket
import time
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from election.election_manager import ElectionManager
from utils.constants import E_FROM_MASTER_PUSH, Q_MASTER_REPLICA, Q_REPLICA_MASTER, ELECTION_PORT
from utils.listener import ReplicaListener
from utils.utils import reanimate_container

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

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

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
            port=ELECTION_PORT
        )

        self.master_alive = Value('i', False)  # 0 = No, 1 = Sí
        self.master_alive_condition = Condition()
        master_coordination_vars = (self.master_alive, self.master_alive_condition)
        self.listener = Process(target=init_listener, args=(id, self.replica_ids, ip_prefix, port, master_coordination_vars,))
        self.listener.start()

        # Determinar si esta réplica es el líder inicial
        self.leader_id = max(self.replica_ids)  # El mayor ID es el líder inicial
        if self.id == self.leader_id:
            logging.info(f"Replica {self.id}: Soy el líder inicial.")

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        self.pull_procesado = False
        try:
            while not self.shutting_down:
                timeout = self._middleware.receive_from_queue_with_timeout(self.recv_queue, self.process_replica_message, self.timeout, auto_ack=False)
                if timeout: self.ask_keepalive()
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

        if self.listener:
            self.listener.terminate()
            self.listener.join()

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

                if self.pull_procesado:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                # Responder con toda la data replicada
                # TODO: Mandar de a batches?
                if self.leader_id == self.id:
                    # self.simulate_failure(3)
                    self._process_pull_data()
                else:
                    # mando keepalive a lider
                    time.sleep(2) 
                    #TODO: esto averiguar como coordinarlo bien. 
                    # Está porque si el lider se cae justo en la linea anterior a self._process_pull_data()
                    # cuando le hacen el keepalive puede responder pero morir al instante y no se detecta
                    # por lo que nunca se responde el pull
                    # el sleep le da tiempo a morir del todo al lider y poder detectarlo
                    self.ask_keepalive_leader()
                    # TODO: ver que pasa si se cae aca el lider.
                    # Si se cae aca el lider despues de que ya me respondio que está vivo tengo un problema.
                    # nadie responde el pull porque no se cambia el lider
                    if self.leader_id == self.id:
                        self._process_pull_data()
                
                self.pull_procesado = True

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")

    def ask_keepalive(self):
        """Verifica si el contenedor maestro está vivo."""
        master_ip = self.container_to_restart  # Dirección IP del contenedor maestro

        if not self.check_node_status(master_ip, self.port):
            logging.info("Master inactivo, manejando el fallo.")
            self.handle_master_down()
        else:
            logging.info("Master activo y accesible.")

    def ask_keepalive_leader(self):
        leader_ip = f"{self.ip_prefix}_{self.leader_id}"
        if not self.check_node_status(leader_ip, self.port):
            logging.info("Leader no disponible, iniciando elección.")
            self.leader_id = self.election_manager.manage_leadership()
            logging.info(f"NUEVO LEADER RECIBIDO: {self.leader_id}")
        else:
            logging.info("Leader activo y accesible.")

    def handle_master_down(self):
        if self.leader_id != self.id:
            self.ask_keepalive_leader() # mando keep_alive -> detecto si se cayo el lider o no
        if self.leader_id == self.id:
            logging.info(f"Replica {self.id}: Soy el líder, ejecutando reanimate_container...")
            reanimate_container(self.container_to_restart)  # Ejecuta la función como líder
            # TODO: Si se me cae aca el lider tengo que ver del lado de las replicas de realizar un timeout y ejecutar una elección.
            # En caso de que se cae, el nuevo lider debe reanimar al master
            self.notify_replicas()  # Notifica a los otros nodos
        else:
            self.wait_for_leader() # espera que el leader responda
        self.pull_procesado = False

    def notify_replicas(self):
        """Envía un mensaje al resto de las réplicas notificando que se ha reanimado el contenedor."""
        for replica_id in self.replica_ids:
            if replica_id != self.id:  # No enviar a sí mismo
                try:
                    logging.info(f"Replica {self.id}: Notificando a réplica {replica_id}...")
                    with socket.create_connection((f"{self.ip_prefix}_{replica_id}", self.port), timeout=1) as sock:
                        msg = SimpleMessage(type=MsgType.MASTER_REANIMATED, socket_compatible=True)
                        sock.sendall(msg.encode())
                        logging.info(f"Replica {self.id}: Notificación enviada a réplica {replica_id}.")
                except socket.timeout:
                    logging.error(f"Replica {self.id}: Timeout al notificar a réplica {replica_id}.")
                except Exception as e:
                    logging.error(f"Error al notificar a réplica {replica_id}: {e}")

    def wait_for_leader(self):
        """Espera a que el líder notifique que se ha ejecutado reanimate_container."""
        # TODO: si el lider se cayó, quedo trabado aca esperando
        with self.master_alive_condition:
            if not self.master_alive.value:
                logging.info("Esperando a que reanimen al master...")
                self.master_alive_condition.wait()  # Espera a que reanimen al master
        logging.info("Master reanimado, sigo")

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


def init_listener(id, replica_ids, ip_prefix, port, master_coordination_vars):
    listener = ReplicaListener(id, replica_ids, ip_prefix, port, master_coordination_vars)
    listener.run()