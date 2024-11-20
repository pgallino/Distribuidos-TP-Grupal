from multiprocessing import Process, Condition, Value
from collections import defaultdict
import logging
import socket
import signal
import subprocess
from messages.messages import ElectionMessage, LeaderElectionMessage, MsgType, OkElectionMessage, PushDataMessage, decode_msg
from replica import Replica
from utils.constants import E_FROM_OS_COUNTER_PUSH, Q_REPLICA_MAIN_PULL, Q_REPLICA_MAIN_PUSH, Q_REPLICA_RESPONSE
from utils.utils import recv_msg

TIMEOUT = 5
PORT = 12345

class OsCounterReplica(Replica):

    def __init__(self, id: int, n_instances: int):
        super().__init__(id, n_instances)
        self._middleware.declare_queue(Q_REPLICA_MAIN_PULL)

        self._middleware.declare_queue(Q_REPLICA_MAIN_PUSH)
        self._middleware.declare_exchange(E_FROM_OS_COUNTER_PUSH, type = 'fanout')
        self._middleware.bind_queue(Q_REPLICA_MAIN_PUSH, E_FROM_OS_COUNTER_PUSH)  # No se necesita `routing_key` para fanout

        # Declarar la cola para enviar respuestas
        self._middleware.declare_queue(Q_REPLICA_RESPONSE)

        self.container_name = "os_counter_1"
        self.condition = Condition()

        self.election_in_progress = Value('i', False)  # 0 = No, 1 = Sí

        # levanto proceso para escuchar por socket
        self.listener_process = Process(target=_start_listener, args=(self.id, self.replica_ids, self.election_in_progress, self.condition, self.container_name))
        self.listener_process.start()

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN_PULL, self.process_replica_message, TIMEOUT, auto_ack=False)
        self.ask_keepalive()

        while True:
            # self._middleware.receive_from_queue(Q_REPLICA_MAIN, self.process_replica_message, auto_ack=False)
            self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN_PUSH, self.process_replica_message, TIMEOUT, auto_ack=False)
            self.ask_keepalive()

        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para OsCounter."""
        self.counters = defaultdict(lambda: (0, 0, 0))  # Diccionario con contadores para Windows, Mac y Linux

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        # logging.info(f"OsCounterReplica: Recibiendo PushDataMessage del cliente {msg.id}")

        # Actualizar los contadores con los datos recibidos
        for client_id, (windows, mac, linux) in msg.data.items():
            self.counters[client_id] = (
                windows,
                mac,
                linux,
            )
        # logging.info(f"OsCounterReplica: Estado actualizado: {self.counters}")

    def _send_data(self):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""
        logging.info(f"OsCounterReplica: Respondiendo a solicitud de PullDataMessage")

        # Crear el mensaje de respuesta con el estado actual
        response_data = PushDataMessage(data=dict(self.counters))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            Q_REPLICA_RESPONSE,  # Cola de respuesta fija
            response_data.encode()
        )
        logging.info("OsCounterReplica: Estado enviado exitosamente a Q_REPLICA_RESPONSE.")

    def _process_fin(self):
        self._shutdown()

    def ask_keepalive(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try: 
            logging.info("Intento conectarme")
            sock.connect(('os_counter_1', 12345))
            logging.info("me conecte, MASTER vivo")
        except (OSError) as e:
            logging.error(f"Conexión rechazada: {e}")

            with self.condition:
                if self.election_in_progress.value:
                    print("Elección ya en proceso. Esperando a que termine...")
                    self.condition.wait()  # Espera a que termine la elección
                    return
                else:
                    # Marcar la elección como en progreso
                    self.election_in_progress.value = True
            initiate_election(self.id, self.replica_ids, self.election_in_progress, self.condition, self.container_name)
        except Exception as e:
            logging.error(f"Error inesperado durante la conexión del socket: {e}")
            if not self.shutting_down:
                logging.error(f"Error inesperado en run: {e}")
        sock.close()

def initiate_election(id, replica_ids, election_in_progress, condition, container_name):
    """Inicia el algoritmo de elección Bully."""
    logging.info(f"Replica {id}: Iniciando elección.")
    higher_replicas = [replica_id for replica_id in replica_ids if replica_id > id]

    if not higher_replicas:
        declare_leader(id, replica_ids, election_in_progress, condition, container_name)
        return

    # Enviar mensajes de elección a réplicas con mayor ID
    for replica_id in higher_replicas:
        try:
            with socket.create_connection((f'os_counter_replica_{replica_id}', PORT + replica_id), timeout=TIMEOUT) as sock:
                sock.sendall(ElectionMessage().encode())
                logging.info(f"Replica {id}: Mensaje de elección enviado a Replica {replica_id}.")
        except (ConnectionRefusedError, socket.timeout):
            logging.warning(f"Replica {id}: No se pudo contactar a Replica {replica_id}.")

    # Esperar respuestas de réplicas con mayor ID
    try:
        with socket.create_server(('', PORT + id)) as server_sock:
            server_sock.settimeout(TIMEOUT)
            conn, _ = server_sock.accept()
            raw_msg = recv_msg(conn)
            msg = decode_msg(raw_msg)
            if msg.type == MsgType.OK_ELECTION:
                logging.info(f"Replica {id}: Respuesta OK recibida, no soy líder.")
                return
    except socket.timeout:
        logging.info(f"Replica {id}: Timeout esperando respuestas, soy líder.")
        declare_leader(id, replica_ids, election_in_progress, condition, container_name)

def declare_leader(id, replica_ids, election_in_progress, condition, container_name):
    """Se declara líder y toma acción para reanimar el maestro."""
    logging.info(f"Replica {id}: Soy el nuevo líder.")
    reanimate_master(id, election_in_progress, condition, container_name)

    # Notificar a las réplicas que es el líder
    for replica_id in replica_ids:
        try:
            with socket.create_connection((f'os_counter_replica_{replica_id}', PORT + replica_id), timeout=TIMEOUT) as sock:
                sock.sendall(LeaderElectionMessage().encode())
                logging.info(f"Replica {id}: Notificación de liderazgo enviada a Replica {replica_id}.")
        except (ConnectionRefusedError, socket.timeout):
            logging.warning(f"Replica {id}: No se pudo contactar a Replica {replica_id}.")

def reanimate_master(id, election_in_progress, condition, container_name):
    """Reanima el contenedor maestro."""
    try:

        container_name = 'os_counter_1'

        # Intentar reiniciar el contenedor
        start_result = subprocess.run(
            ['docker', 'start', container_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logging.info(
            'Docker start executed. Result: {}. Output: {}. Error: {}'.format(
                start_result.returncode,
                start_result.stdout.decode().strip(),
                start_result.stderr.decode().strip()
            )
        )

    except Exception as e:
        logging.error(f"Unexpected error while handling inactive master: {e}")

    finally:
        # Asegurar que la elección se marca como terminada
        with condition:
            election_in_progress.value = False
            condition.notify_all()  # Notificar a otros procesos que la elección terminó
        logging.info(f"Replica {id}: Elección finalizada, election_in_progress=False.")

def _start_listener(id, replica_ids, election_in_progress, condition, container_name):
    """Proceso para escuchar mensajes de otras réplicas."""
    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        listener_socket.bind(('', PORT + id))
        listener_socket.listen(len(replica_ids))
        logging.info(f"Replica {id}: Escuchando en el puerto {PORT + id}.")

        while True:
            try:
                conn, addr = listener_socket.accept()
                raw_msg = recv_msg(conn)
                msg = decode_msg(raw_msg)
                # Mensaje election
                if msg.type == MsgType.ELECTION:
                    # ME LLEGO ELECTION DE UNO, LE RESPONDO OK
                    logging.info(f"Replica {id}: llego un mensaje Ok: {msg}")
                    conn.sendall(OkElectionMessage().encode())
                    with condition:
                        if election_in_progress.value:
                            continue
                        else:
                            election_in_progress.value = True
                    initiate_election(id, replica_ids, election_in_progress, condition, container_name)
                elif msg.type == MsgType.LEADER_ELECTION:
                    # ME LLEGO UN LEADER LISTO
                    logging.info(f"Replica {id}: Llego un mensaje Leader: {msg}")
                conn.close()
            except socket.error as e:
                logging.error(f"Replica {id}: Error en el socket de escucha: {e}")
    except Exception as e:
        logging.error(f"Replica {id}: Error iniciando el servidor socket: {e}")
    finally:
        listener_socket.close()


# (MAIN) (LISTENER)
# * Si recibo un election alguien menor que yo se dio
#   cuenta que se cayo el maestro, tengo que seguir con el algoritmo. Le respondo ok (LISTENER)
# * Si recibo un leader, me desentiendo del algoritmo. (MAIN) y (LISTENER)
# * Si recibo ok, me quedo esperando por el leader de otra replica. (MAIN)
# * Si me di cuenta que se cayó el maestro y no soy el mayor,
#   arranco con el algoritmo mandando election a todos los mayores
#   que yo. (MAIN)
# * Si me di cuenta que se cayó el maestro y soy el mayor, mando a
#   todos el leader. (MAIN)
# * Si despues de un tiempo no me responden, soy el mayor, mando
#   leader a todos las replicas. (MAIN)