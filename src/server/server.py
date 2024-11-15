from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Condition, Process, Lock, Manager
import socket
import logging
import signal

from result_dispatcher import ResultDispatcher
from connection_handler import ConnectionHandler
from utils.constants import Q_QUERY_RESULT_1, Q_QUERY_RESULT_2, Q_QUERY_RESULT_3, Q_QUERY_RESULT_4, Q_QUERY_RESULT_5

DISPATCH_QUEUES = 5

def handle_client_connection(client_id: int, client_socket: socket.socket, n_next_nodes: int):
    connection_handler = ConnectionHandler(client_id, client_socket, n_next_nodes)
    connection_handler.run()

def init_result_dispatcher(client_sockets, lock, space_available, result_queue):
    dispatcher = ResultDispatcher(client_sockets, lock, space_available, result_queue)
    dispatcher.listen_to_queue()

class Server:

    def __init__(self, port, listen_backlog, n_next_nodes: int):

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.shutting_down = False
        self.n_next_nodes = n_next_nodes

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(5)


        self.dispatchers = []
        self.max_connections = listen_backlog
        self.manager = Manager()
        self.active_connections = self.manager.dict()  # Shared dictionary for client connections
        self.active_connections_lock = Lock()
        self.handler_pool = ProcessPoolExecutor(max_workers=listen_backlog)
        # Condition variable para sincronización entre procesos
        self.space_available = Condition()

        self.client_id_counter = 0  # Inicialización del contador

        # Inicialización de las pools

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        logging.info("action: Received SIGTERM | shutting down server.")
        self._shutdown()
    
    def _shutdown(self):
        if self.shutting_down:
            return
        
        logging.info("action: shutdown SERVER | result: in progress...")
        self.shutting_down = True


        self._server_socket.close()

        # Cerrar la pool
        self.handler_pool.shutdown(wait=True)

        # Terminar y unir todos los result dispatchers
        for dispatcher in self.dispatchers:
            if dispatcher:
                if dispatcher.is_alive():
                    dispatcher.terminate()
                    dispatcher.join()
                    logging.info("action: close dispatcher | result: success")

        self.manager.shutdown()
        self.notification_queue.close()

        logging.info("action: shutdown | result: success")

    def start_dispatchers(self):
        """Inicializa los dispatchers antes de aceptar conexiones."""
        queues = [
            Q_QUERY_RESULT_1,
            Q_QUERY_RESULT_2,
            Q_QUERY_RESULT_3,
            Q_QUERY_RESULT_4,
            Q_QUERY_RESULT_5
        ]

        for queue_name in queues:
            process = Process(target=init_result_dispatcher, args=(self.active_connections, self.active_connections_lock, self.space_available, queue_name,))
            process.start()
            self.dispatchers.append(process)

    def run(self):
        """Server loop to accept and handle new client connections."""

        self.start_dispatchers()

        try:

            while True:
                        
                # Solo bloquear si alcanzamos el límite de conexiones
                with self.space_available:
                    if len(self.active_connections) >= self.max_connections:                        
                        logging.info("Se alcanzó el límite de conexiones, esperando espacio...")
                        self.space_available.wait()  # Espera hasta recibir una señal de espacio libre
                        logging.info("Conseguí espacioooooo")
            
                client_socket = self._accept_new_connection()
                self.client_id_counter += 1 # TODO me parece más logico que el server sea el que decida los ids, no que le llegue
                client_id = self.client_id_counter

                # Asignar la conexión a la pool de handlers
                self.handler_pool.submit(handle_client_connection, client_id, client_socket, self.n_next_nodes)

                with self.active_connections_lock:
                    self.active_connections[client_id] = (client_socket, 0)  # Track client socket by ID

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: run | result: fail | error: {e}")
                self._shutdown()  # Trigger shutdown on error

    def _accept_new_connection(self):
        logging.info('action: accept_connections | result: in progress...')
        client, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client
