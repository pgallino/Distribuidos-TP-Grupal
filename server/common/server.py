from messages.messages import QueryNumber, decode_msg, decode_msg, Result, MsgType
from middleware.middleware import Middleware

import socket
import logging
import signal
from utils.utils import safe_read, recv_msg

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
Q_QUERY_RESULT_1 = "query_result_1"
Q_QUERY_RESULT_2 = "query_result_2"
Q_QUERY_RESULT_3 = "query_result_3"
Q_QUERY_RESULT_4 = "query_result_4"
Q_QUERY_RESULT_5 = "query_result_5"

class Server:

    def __init__(self, port, listen_backlog):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_queue(Q_QUERY_RESULT_1)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)
        self._middleware.declare_queue(Q_QUERY_RESULT_3)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_queue(Q_QUERY_RESULT_5)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._server_socket.close()
        self._middleware.connection.close()

    def run(self):
        """Server loop to accept and handle new client connections."""
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        while True:
            try:
                client_socket = self._accept_new_connection()
                self.__handle_client_connection(client_socket)
                break
            except OSError as error:
                if not self.shutting_down:
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
                # self.logger.custom(f"action: receive_message | result: success | {msg}")

                # Enviamos el mensaje ya codificado directamente a la cola
                self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, msg.encode())
                if msg.type == MsgType.FIN:
                    break
            self._listen_to_result_queues()
        except ValueError as e:
            if self.shutting_down:
                return
            # Captura el ValueError y loggea el cierre de la conexión sin lanzar error
            self.logger.custom(f"Connection closed or invalid message received: {e}")
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self.logger.custom(f"action: ending_connection | result: success")

    def _listen_to_result_queues(self):
        """Listen to multiple queues for result messages and print the results."""
        self.logger.custom("action: listen_to_queues | result: in_progress")
        
        queues = [Q_QUERY_RESULT_1, Q_QUERY_RESULT_2, Q_QUERY_RESULT_3, Q_QUERY_RESULT_5, Q_QUERY_RESULT_4]
        
        try:
            for queue in queues:
                raw_message = self._middleware.receive_from_queue(queue)
                msg = decode_msg(raw_message)
                
                # Verifica si el mensaje es del tipo Result
                if msg.type == MsgType.RESULT:
                    # Imprime los resultados de acuerdo al tipo específico utilizando result_type
                    if msg.result_type == QueryNumber.Q1:
                        self.logger.custom(
                            f"Received Result from {queue}: OS Count Summary:\n"
                            f"Windows: {msg.windows_count}\n"
                            f"Mac: {msg.mac_count}\n"
                            f"Linux: {msg.linux_count}\n"
                        )
                    
                    elif msg.result_type == QueryNumber.Q2:
                        top_games_str = "\n".join(f"- {name}: {playtime} average playtime" for name, playtime in msg.top_games)
                        self.logger.custom(
                            f"Received Result from {queue}: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
                            f"{top_games_str}\n"
                        )
                    
                    elif msg.result_type == QueryNumber.Q3:
                        indie_games_str = "\n".join(f"{rank}. {name}: {reviews} positive reviews" 
                                                    for rank, (name, reviews) in enumerate(msg.top_indie_games, start=1))
                        self.logger.custom(
                            f"Received Result from {queue}: Q3: Top 5 Indie Games with Most Positive Reviews:\n"
                            f"{indie_games_str}\n"
                        )
                    
                    elif msg.result_type == QueryNumber.Q4:
                        negative_reviews_str = "\n".join(f"- {name}: {count} negative reviews" for name, count in msg.negative_reviews)
                        self.logger.custom(
                            f"Received Result from {queue}: Q4: Action games with more than 5,000 negative reviews in English:\n"
                            f"{negative_reviews_str}\n"
                        )
                    
                    elif msg.result_type == QueryNumber.Q5:
                        top_negative_str = "\n".join(f"- {name}: {count} negative reviews" for name, count in msg.top_negative_reviews)
                        self.logger.custom(
                            f"Received Result from {queue}: Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
                            f"{top_negative_str}\n"
                        )
                    
                    else:
                        self.logger.custom(f"Received Unknown Result Type from {queue}: {msg}")
                else:
                    self.logger.custom(f"Received Message from {queue}: {msg}")        
        except ValueError as e:
            if not self.shutting_down:
                self.logger.custom(f"Error decoding message from {queue}: {e}")
        except OSError as e:
            if not self.shutting_down:
                logging.error(f"Error receiving from {queue}: {e}")
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")


