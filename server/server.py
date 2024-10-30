from multiprocessing import Process, Queue, Manager
import socket
import logging
import signal

from result_dispatcher import ResultDispatcher
from connection_handler import ConnectionHandler
from utils.constants import Q_QUERY_RESULT_1, Q_QUERY_RESULT_2, Q_QUERY_RESULT_3, Q_QUERY_RESULT_4, Q_QUERY_RESULT_5

def handle_client_connection(client_id: int, client_socket: socket.socket, n_next_nodes: int):
    connection_handler = ConnectionHandler(client_id, client_socket, n_next_nodes)
    connection_handler.run()

def init_result_dispatcher(client_connections, notification_queue, result_queue):
    dispatcher = ResultDispatcher(client_connections, notification_queue, result_queue)
    dispatcher.listen_to_queue()

class Server:

    def __init__(self, port, listen_backlog, n_next_nodes: int):

        self.logger = logging.getLogger(__name__)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.shutting_down = False
        self.n_next_nodes = n_next_nodes

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        
        self.handlers = []
        self.dispatchers = []
        self.manager = Manager()
        self.client_connections = self.manager.dict()  # Managed dictionary for client_id -> {client_id: (conn, n_results_received)}
        self.connections_limit = listen_backlog
        self.notification_queue = Queue()   # Queue to receive termination notifications
        self.dispatcher_process = None  # Variable para el proceso del dispatcher
        self.client_id_counter = 0  # Inicialización del contador

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("action: Received SIGTERM | shutting down server.")
        self._shutdown()
    
    def _shutdown(self):
        if self.shutting_down:
            return
        
        self.logger.custom("action: shutdown SERVER | result: in progress...")
        self.shutting_down = True


        self._server_socket.close()

        # Terminar y unir todos los result dispatchers
        for dispatcher in self.dispatchers:
            if dispatcher:
                if dispatcher.is_alive():
                    self.logger.custom("action: cierro dispatcher")
                    dispatcher.terminate()
                    dispatcher.join()

        # Terminar y unir todos los connection handlers
        for handler in self.handlers:
            if handler:
                if handler.is_alive():
                    self.logger.custom("action: cierro handler")
                    handler.terminate()
                    handler.join()

        self.manager.shutdown()
        self.notification_queue.close()

        self.logger.custom("action: shutdown | result: success")

    def run(self):
        """Server loop to accept and handle new client connections."""

        queues = [
            Q_QUERY_RESULT_1,
            Q_QUERY_RESULT_2,
            Q_QUERY_RESULT_3,
            Q_QUERY_RESULT_4,
            Q_QUERY_RESULT_5
        ]

        try:
            for queue_name in queues:
                process = Process(target=init_result_dispatcher, args=(self.client_connections, self.notification_queue, queue_name,))
                process.start()
                self.dispatchers.append(process)

            while True:
                        
                if len(self.client_connections) >= self.connections_limit:
                    # Blocking wait for a child process to finish
                    finished_client_id = self.notification_queue.get()
                    # Remove finished child process from the list
                    if finished_client_id in self.client_connections:
                        del self.client_connections[finished_client_id] 
                    self.logger.info(f"Slot freed up by process {finished_client_id}. Accepting new connections.")
                
                client_socket = self._accept_new_connection()
                self.client_id_counter += 1 # TODO me parece más logico que el server sea el que decida los ids, no que le llegue
                client_id = self.client_id_counter

                child_process = Process(
                    target=handle_client_connection,
                    args=(client_id, client_socket, self.n_next_nodes)
                )

                child_process.start()
                self.client_connections[client_id] = (client_socket, 0)  # Track client socket by ID
                self.handlers.append(child_process)

        except Exception as e:
            if not self.shutting_down:
                self.logger.custom(f"action: run | result: fail | error: {e}")
                self._shutdown()  # Trigger shutdown on error

    def _accept_new_connection(self):
        self.logger.custom('action: accept_connections | result: in progress...')
        client, addr = self._server_socket.accept()
        self.logger.custom(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client
