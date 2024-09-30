from middleware.middleware import Middleware

import socket
import logging
import signal
import os

def safe_read(socket):
    data = bytearray()
    # while len(data) < n_bytes:
    data = socket.recv(1024)
        # if not packet:
            # return None
        # data += packet
    return data

class Server:

    def __init__(self, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._middleware = Middleware()
        self._middleware.declare_queue('general_queue')

    def _handle_sigterm(self, sig, frame):
        """
        Handle SIGTERM signal so the server close gracefully.
        """
        if not self.clean:
            self.clean_up()
        self._server_socket.close()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        while True:
            try:
                client_socket = self._accept_new_connection()
                self.__handle_client_connection(client_socket)
            except OSError as error:
                break


    def _accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        client, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return client
    
    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            while True:
                logging.info(f'action: receive_message | result: in_progress')
                # msg_type, data = recv_message(client_sock)
                data = safe_read(client_sock)
                msg = data.decode()
                logging.info(f"action: receive_message | result: success | client_msg: {msg}")
                if msg == "FIN":
                    logging.info(f"action: ending_connection | result: in_progress")
                    break
                self._middleware.send_to_queue("general_queue", msg)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            logging.info(f"action: ending_connection | result: success")