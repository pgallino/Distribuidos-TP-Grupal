from messages.messages import decode_msg, encode_data, encode_fin, encode_handshake
from middleware.middleware import Middleware

import socket
import logging
import signal
from utils.utils import safe_read, recv_msg


class Server:

    def __init__(self, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._middleware = Middleware()
        self._middleware.declare_queue('general_queue')

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        logging.info("Received SIGTERM, shutting down server.")
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
        logging.info('action: accept_connections | result: in_progress')
        client, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client

    def __handle_client_connection(self, client_sock):
        """Handle communication with a connected client."""
        try:
            while True:
                raw_msg = recv_msg(client_sock)
                msg = decode_msg(raw_msg)
                logging.info(f"action: receive_message | result: succes | {msg}")
                # Condicionales para manejar diferentes tipos de mensajes
                if msg['tipo'] == 'handshake':
                    # Si el mensaje es de tipo 'handshake', lo codificamos y enviamos a la cola
                    encoded_msg = encode_handshake(msg['id'])
                    self._middleware.send_to_queue("general_queue", encoded_msg)

                elif msg['tipo'] == 'data':
                    # Si el mensaje es de tipo 'data', lo codificamos y enviamos a la cola
                    encoded_msg = encode_data(msg['id'], msg['data'])
                    self._middleware.send_to_queue("general_queue", encoded_msg)

                elif msg['tipo'] == 'fin':
                    # Si el mensaje es de tipo 'fin', lo codificamos y enviamos a la cola
                    encoded_msg = encode_fin(msg['id'])
                    self._middleware.send_to_queue("general_queue", encoded_msg)

                else:
                    logging.error(f"Unknown message type: {msg['tipo']}")
        except ValueError as e:
            # Captura el ValueError y loggea el cierre de la conexión sin lanzar error
            logging.info(f"Connection closed or invalid message received: {e}")
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            logging.info(f"action: ending_connection | result: success")

