import logging
import signal
import socket
from messages.messages import MsgType, SimpleMessage, decode_msg
from utils.container_constants import LISTENER_PORT
from utils.utils import TaskType, recv_msg

class Listener:
    def __init__(self, id, ip_prefix, port=LISTENER_PORT, backlog=5):
        """
        Inicializa el manejador de Keep Alive.

        :param ip_prefix: Nombre o dirección del contenedor/nodo.
        :param port: Puerto donde se escuchan los mensajes Keep Alive.
        :backlog: Cantidad de conexiones en el backlog
        """
        self.id = id
        self.ip_prefix = ip_prefix
        self.port = port
        self.backlog = backlog
        self.shutting_down = False
        self.conn = None
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((f'{self.ip_prefix}_{self.id}', self.port))
        self.sock.listen(self.backlog)
        logging.info(f"KeepAliveHandler: Escuchando mensajes en {f'{self.ip_prefix}_{self.id}'}:{self.port}")

    def shutdown(self):
        self.shutting_down = True
        if self.sock:
            self.sock.close()

        if self.conn:
            self.conn.close()

    def handle_sigterm(self, sig, frame):
        """Manejador para SIGTERM."""
        self.shutdown()

    def process_msg(self, conn):
        raise NotImplementedError("Implementacion a cargo de subclases")
    
    def run(self):
        """Proceso dedicado a manejar mensajes de Keep Alive."""

        while not self.shutting_down:
            try:
                self.conn, addr = self.sock.accept()
                self.process_msg(self.conn)
                logging.info(f"KeepAliveHandler: Conexión recibida de {addr}")
                self.conn.close()
            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"KeepAliveHandler: Error manejando conexión: {e}")
                    self.shutdown()
                    logging.info("KeepAliveHandler: Proceso terminado.")

class ReplicaListener(Listener):
    def __init__(self, id, ids, ip_prefix, port, task_coordination_vars, leader_id):
        super().__init__(id, ip_prefix, port, 5)

        self.leader_id = leader_id  # Variable compartida para el id del líder
        self.task_status, self.task_condition = task_coordination_vars
        self.node_ids = ids

    def process_msg(self, conn):
        raw_msg = recv_msg(conn)
        msg = decode_msg(raw_msg)

        if msg.type == MsgType.KEEP_ALIVE:
            pass

        elif msg.type == MsgType.TASK_INTENT:
            task_type = TaskType(msg.task_type)
            logging.info(f"Replica {self.id}: Recibí TASK_INTENT para tarea '{task_type.name}'.")
            
            # Actualizar task_status y notificar
            with self.task_condition:
                self.task_status["intent"] = task_type.value
                self.task_condition.notify_all()

        elif msg.type == MsgType.TASK_COMPLETED:
            task_type = TaskType(msg.task_type)
            logging.info(f"Replica {self.id}: Recibí TASK_COMPLETED para tarea '{task_type.name}'.")
            
            # Actualizar task_status y notificar
            with self.task_condition:
                self.task_status["completed"] = task_type.value
                self.task_condition.notify_all()

        elif msg.type == MsgType.WHO_IS_LEADER:
            # Responder con el líder actual
            current_leader = self.leader_id.value  # Obtener el líder actual
            response_msg = SimpleMessage(type=MsgType.CURRENT_LEADER, current_leader=current_leader, socket_compatible=True)
            conn.sendall(response_msg.encode())
            logging.info(f"Replica {self.id}: Respondí que el líder actual es {current_leader}.")

            
class NodeListener(Listener):
    def __init__(self, id, ip_prefix, connected, port=LISTENER_PORT, backlog=5):
        super().__init__(id, ip_prefix, port, backlog)

        self.connected = connected

    def process_msg(self, conn):
        raw_msg = recv_msg(conn)
        msg = decode_msg(raw_msg)

        if msg.type == MsgType.KEEP_ALIVE:
            pass
        elif msg.type == MsgType.ASK_MASTER_CONNECTED:
            # Construir y enviar la respuesta con el estado de conexión
            response_msg = SimpleMessage(type=MsgType.MASTER_CONNECTED, socket_compatible=True, connected=self.connected.value) # 0 para False, 1 para True
            conn.sendall(response_msg.encode())
            logging.info(f"NodeListener: Respondí con estado 'connected={self.connected}'.")