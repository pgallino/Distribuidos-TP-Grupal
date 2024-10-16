from messages.messages import decode_msg, MsgType
from messages.results_msg import QueryNumber
from middleware.middleware import Middleware

import socket
import logging
import signal
from utils.constants import Q_GATEWAY_TRIMMER, Q_QUERY_RESULT_1, Q_QUERY_RESULT_2, Q_QUERY_RESULT_3, Q_QUERY_RESULT_4, Q_QUERY_RESULT_5
from utils.utils import recv_msg

class Server:

    def __init__(self, port, listen_backlog, n_next_nodes: int):

        self.logger = logging.getLogger(__name__)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        self.shutting_down = False
        self.n_next_nodes = n_next_nodes

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._middleware = Middleware()
        self._middleware.declare_queues([Q_GATEWAY_TRIMMER,
                                         Q_QUERY_RESULT_1, 
                                         Q_QUERY_RESULT_2, 
                                         Q_QUERY_RESULT_3, 
                                         Q_QUERY_RESULT_4, 
                                         Q_QUERY_RESULT_5])
        self.client_sock = None

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self._shutdown()
    
    def _shutdown(self):
        if self.shutting_down:
            return
        self.logger.custom("action: shutdown | result: in progress...")
        self.shutting_down = True
        self._server_socket.close()
        self._middleware.close()
        self.logger.custom("action: shutdown | result: success")

    def run(self):
        """Server loop to accept and handle new client connections."""

        try:
            while True:
                try:
                    client_socket = self._accept_new_connection()
                    self.__handle_client_connection(client_socket)
                    break
                except OSError as error:
                    if not self.shutting_down:
                        logging.error(f"Server error: {error}")
                        self._shutdown()
                    break
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: run | result: fail | error: {e}")
        finally:
            self._shutdown()

    def _accept_new_connection(self):
        """Accept new client connections."""
        self.logger.custom('action: accept_connections | result: in progress...')
        client, addr = self._server_socket.accept()
        self.logger.custom(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client

    def __handle_client_connection(self, client_sock):
        """Handle communication with a connected client."""
        try:
            self.client_sock = client_sock
            while True:
                raw_msg = recv_msg(client_sock)
                msg = decode_msg(raw_msg)
                
                # self.logger.custom(f"action: receive_message | result: success | {msg}")

                # Enviamos el mensaje ya codificado directamente a la cola
                if msg.type == MsgType.DATA:
                    self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, msg.encode())
                elif msg.type == MsgType.FIN:
                    for _ in range(self.n_next_nodes):
                        self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, msg.encode())
                    break
            self._listen_to_result_queues()
        except ValueError as e:
            self.logger.custom(f"Connection closed or invalid message received: {e}")
        except OSError as e:
            if not self.shutting_down:
                logging.error(f"action: receive_message | result: fail | error: {e}")
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

    def _listen_to_result_queues(self):
        """Listen to multiple queues for result messages using callbacks."""
        self.logger.custom("action: listen to result queues | result: in progress...")

        queues = [
            Q_QUERY_RESULT_1,
            Q_QUERY_RESULT_2,
            Q_QUERY_RESULT_3,
            Q_QUERY_RESULT_4,
            Q_QUERY_RESULT_5
        ]

        for queue_name in queues:
            self._middleware.receive_from_queue(queue_name, self._process_result_callback)


    def _process_result_callback(self, ch, method, properties, body):
        """Callback to process messages from result queues."""
        try:
            self.client_sock.sendall(body)
            self._middleware.channel.stop_consuming()

        except ValueError as e:
            self.logger.custom(f"Error decoding message from {method.routing_key}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to process message from {method.routing_key}: {e}")

