import logging
import signal
import socket
from multiprocessing import Process
from election.election_logic import initiate_election
from messages.messages import MsgType, OkElectionMessage, decode_msg
from utils.utils import recv_msg

PORT = 12345

class ElectionListener:
    def __init__(self, id, replica_ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart):
        self.id = id
        self.replica_ids = replica_ids
        self.container_name = container_name
        self.election_in_progress = election_in_progress
        self.condition = condition
        self.waiting_ok_election = waiting_ok_election
        self.condition_ok = condition_ok
        self.on_leader_selected = on_leader_selected
        self.container_to_restart = container_to_restart
        self.port = PORT
        self.listener_socket = None
        self.process = None

        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """Proceso que escucha mensajes de otras réplicas."""
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.listener_socket.bind(('', self.port))
            self.listener_socket.listen(len(self.replica_ids))
            logging.info(f"Replica {self.id}: Escuchando en el puerto {self.port}.")

            while True:
                try:
                    conn, _ = self.listener_socket.accept()
                    raw_msg = recv_msg(conn)
                    msg = decode_msg(raw_msg)

                    if msg.type == MsgType.ELECTION:
                        self._handle_election_message(msg)

                    elif msg.type == MsgType.LEADER_ELECTION:
                        self._handle_leader_election_message()

                    elif msg.type == MsgType.OK_ELECTION:
                        self._handle_ok_election_message()

                    conn.close()

                except socket.error as e:
                    logging.error(f"Replica {self.id}: Error en el socket de escucha: {e}")
        except Exception as e:
            logging.error(f"Replica {self.id}: Error iniciando el servidor socket: {e}")
        finally:
            self.listener_socket.close()

    def _handle_election_message(self, msg):
        """Procesa un mensaje de tipo ELECTION."""
        logging.info(f"Replica {self.id}: Llego un mensaje Election")
        response_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            response_socket.connect((f'{self.container_name}_{msg.id}', self.port))
            response_socket.sendall(OkElectionMessage(id=self.id).encode())
        finally:
            response_socket.close()

        start_election = False

        with self.condition:
            if not self.election_in_progress.value:
                self.election_in_progress.value = True
                start_election = True
                logging.info("ARRANQUE ELECCION EN LISTENER")

        if start_election:
            self.process = Process(target=self._initiate_election)
            self.process.start()

    def _handle_leader_election_message(self):
        """Procesa un mensaje de tipo LEADER_ELECTION."""
        logging.info(f"Replica {self.id}: Llego un mensaje Leader")
        with self.condition:
            self.election_in_progress.value = False
            self.condition.notify_all()

        if self.process:
            self.process.join()

    def _handle_ok_election_message(self):
        """Procesa un mensaje de tipo OK_ELECTION."""
        logging.info(f"Replica {self.id}: Llego un mensaje Ok")
        with self.condition_ok:
            self.waiting_ok_election.value = False

    def _initiate_election(self):
        """Inicia el proceso de elección llamando a la lógica principal."""
        initiate_election(self.id, self.replica_ids, self.container_name, self.election_in_progress, self.condition,
                          self.waiting_ok_election, self.condition_ok, self.on_leader_selected, self.container_to_restart)
        
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
