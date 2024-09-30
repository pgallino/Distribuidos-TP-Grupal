from middleware.middleware import Middleware

import socket
import logging
import signal

def safe_read(socket):
    """Función que lee datos del socket, devolviendo mensajes completos uno por vez."""
    buffer = bytearray()
    try:
        while True:
            chunk = socket.recv(1024)
            if not chunk:
                logging.info("No data received, client may have closed the connection.")
                return None
            buffer.extend(chunk)

            # Procesar todos los mensajes que contengan '\n'
            if b'\n' in buffer:
                messages = buffer.split(b'\n')  # Separar mensajes completos
                for msg in messages[:-1]:  # Procesar todos menos el último incompleto
                    yield msg.decode('utf-8').strip()
                buffer = messages[-1]  # Guardar el último mensaje incompleto en el buffer
    except OSError as e:  # Aquí cambiamos socket.error por OSError
        logging.error(f"Error receiving data: {e}")
        return None


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
            for msg in safe_read(client_sock):
                if msg is None:
                    logging.info("Client closed the connection or no data.")
                    break

                logging.info(f"action: receive_message | result: success | client_msg: '{msg}'")

                if msg == "FIN":
                    logging.info(f"action: ending_connection | result: in_progress")
                    self._middleware.send_to_queue("general_queue", msg)
                    break

                self._middleware.send_to_queue("general_queue", msg)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            logging.info(f"action: ending_connection | result: success")

