import logging
from multiprocessing import Process
import socket
from election.election_logic import initiate_election
from listener.listener import Listener
from messages.messages import MsgType, SimpleMessage, decode_msg
from utils.container_constants import LISTENER_PORT
from utils.utils import recv_msg


class WatchDogListener(Listener):

    def __init__(self, id, ip_prefix, n_watchdogs, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id, port=LISTENER_PORT, backlog=5):
        super().__init__(id, ip_prefix, port, backlog)
        self.n_watchdogs = n_watchdogs
        self.leader_id = leader_id
        self.election_in_progress = election_in_progress
        self.election_condition = election_condition
        self.waiting_ok = waiting_ok
        self.ok_condition = ok_condition
        self.election_process = None

    def process_msg(self, conn):
        raw_msg = recv_msg(conn)
        msg = decode_msg(raw_msg)

        if msg.type == MsgType.KEEP_ALIVE:
            pass

        elif msg.type == MsgType.ELECTION:
            self._handle_election_message(msg)

        elif msg.type == MsgType.LEADER_ELECTION:
            self._handle_leader_election_message(msg)

        elif msg.type == MsgType.OK_ELECTION:
            self._handle_ok_election_message()
    
    def _handle_election_message(self, msg):
        """Procesa un mensaje de tipo ELECTION."""
        logging.info(f"node {self.id}: Llego un mensaje Election de {msg.node_id}")
        try:
            with socket.create_connection((f'{self.ip_prefix}_{msg.node_id}', self.port), timeout=3) as response_socket:
                ok_msg = SimpleMessage(type=MsgType.OK_ELECTION, socket_compatible=True, node_id=self.id)
                response_socket.sendall(ok_msg.encode())
                logging.info(f"node {self.id}: Enviado OK a {self.ip_prefix}_{msg.node_id}:{self.port}")
        except socket.gaierror as e:
            logging.warning(f"node {self.id}: Error en resolución de nombre al responder a {msg.node_id}: {e}")
        except (socket.timeout, ConnectionRefusedError) as e:
            logging.warning(f"node {self.id}: Nodo {msg.node_id} no está disponible: {e}")
        except Exception as e:
            logging.error(f"node {self.id}: Error inesperado al manejar mensaje Election: {e}")

        start_election = False

        with self.election_condition:
            if not self.election_in_progress.value:
                self.election_in_progress.value = True
                start_election = True

        if start_election:
            self.election_process = Process(target=initiate_election, args=(self.id,  [id for id in range(1, self.n_watchdogs+1)], self.ip_prefix, self.election_in_progress, self.election_condition, self.waiting_ok, self.ok_condition, self.leader_id))
            self.election_process.start()

    def _handle_leader_election_message(self, msg):
        """Procesa un mensaje de tipo LEADER_ELECTION."""
        logging.info(f"node {self.id}: Llego un mensaje Leader")
        with self.election_condition:
            self.leader_id.value = msg.node_id
            self.election_in_progress.value = False
            self.election_condition.notify_all()

        if self.election_process:
            self.election_process.join()

    def _handle_ok_election_message(self):
        """Procesa un mensaje de tipo OK_ELECTION."""
        logging.info(f"node {self.id}: Llego un mensaje Ok")
        with self.ok_condition:
            self.waiting_ok.value = False
            self.ok_condition.notify_all()
        
        logging.info(f"node {self.id}: notifiqué el ok")

    def stop(self):
        """Detiene el listener y libera recursos."""
        if self.listener_socket:
            self.listener_socket.close()
            self.listener_socket = None

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return

        logging.info("action: shutdown_election_listener | result: in progress...")
        self.shutting_down = True

        self.stop()

    def _handle_sigterm(self, sig, frame):
        """Maneja la señal SIGTERM para cerrar la réplica de forma segura."""
        logging.info("action: Received SIGTERM election listener | shutting down gracefully.")
        self._shutdown()