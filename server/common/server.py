from messages.messages import decode_msg, decode_msg
from middleware.middleware import Middleware

import socket
import logging
import signal
from utils.utils import safe_read, recv_msg

Q_GATEWAY_TRIMMER = 'gateway-trimmer'

class Server:

    def __init__(self, port, listen_backlog):

        self.logger = logging.getLogger(__name__)

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self._server_socket.close()

    def run(self):
        """Server loop to accept and handle new client connections."""
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        while True:
            try:
                client_socket = self._accept_new_connection()
                self.__handle_client_connection(client_socket)
            except OSError as error:
                logging.error(f"Server error: {error}")
                break

    def _accept_new_connection(self):
        """Accept new client connections."""
        self.logger.custom('action: accept_connections | result: in_progress')
        client, addr = self._server_socket.accept()
        self.logger.custom(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client

    def __handle_client_connection(self, client_sock):
        """Handle communication with a connected client."""
        try:
            while True:
                raw_msg = recv_msg(client_sock)
                msg = decode_msg(raw_msg)  # Ahora devuelve directamente un objeto Handshake, Data o Fin
                self.logger.custom(f"action: receive_message | result: success | {msg}")

                # Enviamos el mensaje ya codificado directamente a la cola
                encoded_msg = msg.encode()
                self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, encoded_msg)
        except ValueError as e:
            # Captura el ValueError y loggea el cierre de la conexión sin lanzar error
            self.logger.custom(f"Connection closed or invalid message received: {e}")
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            self.logger.custom(f"action: ending_connection | result: success")

