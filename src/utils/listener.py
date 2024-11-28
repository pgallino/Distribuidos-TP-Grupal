import logging
import signal
import socket
from messages.messages import MsgType, decode_msg
from utils.utils import recv_msg

class Listener:
    def __init__(self, id, ip_prefix, port=12345, backlog=5):
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
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def shutdown(self):
        self.shutting_down = True
        if self.sock:
            self.sock.close()

    def handle_sigterm(self, sig, frame):
        """Manejador para SIGTERM."""
        self.shutdown()

    def process_msg(self, conn):
        raise NotImplementedError("Implementacion a cargo de subclases")
    
    def run(self):
        """Proceso dedicado a manejar mensajes de Keep Alive."""
        
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((f'{self.ip_prefix}_{self.id}', self.port))
            self.sock.listen(self.backlog)
            logging.info(f"KeepAliveHandler: Escuchando mensajes en {f'{self.ip_prefix}_{self.id}'}:{self.port}")
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"KeepAliveHandler: Error en proceso: {e}")
                self.shutdown()
                return

        while True:
            try:
                conn, addr = self.sock.accept()
                self.process_msg(conn)
                logging.info(f"KeepAliveHandler: Conexión recibida de {addr}")
                conn.close()
            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"KeepAliveHandler: Error manejando conexión: {e}")
                    self.shutdown()
                    logging.info("KeepAliveHandler: Proceso terminado.")

class ReplicaListener(Listener):
    def __init__(self, id, ids, ip_prefix, port, master_coordination_vars):
        super().__init__(id, ip_prefix, port, 5)

        self.master_alive, self.master_alive_condition = master_coordination_vars
        self.node_ids = ids

    def process_msg(self, conn):
        raw_msg = recv_msg(conn)
        msg = decode_msg(raw_msg)

        if msg.type == MsgType.KEEP_ALIVE:
            pass
        if msg.type == MsgType.MASTER_REANIMATED:
            logging.info(f"Replica {self.id}: Recibí mensaje MASTER_REANIMATED.")
            with self.master_alive_condition:  # Usar la condición para actualizar master_alive
                self.master_alive.value = True
                self.master_alive_condition.notify_all()  # Notificar a los hilos en espera
                logging.info(f"Replica {self.id}: master_alive cambiado a True.")

            
class NodeListener(Listener):
    def __init__(self, id, ip_prefix, port=12345, backlog=5):
        super().__init__(id, ip_prefix, port, backlog)

    def process_msg(self, conn):
        return 
        # raw_msg = recv_msg(conn)
        # msg = decode_msg(raw_msg)

        # if msg.type == MsgType.KEEP_ALIVE:
        #     pass
