import logging
from multiprocessing import Process
import signal
import socket
from messages.messages import MsgType, SimpleMessage, decode_msg
from utils.container_constants import LISTENER_PORT
from utils.utils import recv_msg

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
        raw_msg = recv_msg(conn)
        msg = decode_msg(raw_msg)

        if msg.type == MsgType.KEEP_ALIVE:
            conn.sendall(SimpleMessage(type=MsgType.ALIVE, socket_compatible=True).encode())
    
    def run(self):
        """Proceso dedicado a manejar mensajes de Keep Alive."""

        while not self.shutting_down:
            try:
                conn, addr = self.sock.accept()
                # logging.info(f"[Listener] Conexion recibida")
                self.process_msg(conn)
                conn.close()
            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"KeepAliveHandler: Error manejando conexión: {e}")
                    self.shutdown()
                    logging.info("KeepAliveHandler: Proceso terminado.")
