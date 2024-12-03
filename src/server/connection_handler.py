import logging
import signal
from messages.messages import Data, MsgType, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.middleware_constants import Q_GATEWAY_TRIMMER
from utils.utils import recv_msg


class ConnectionHandler:
    """Handles communication with a connected client in a separate process."""

    def __init__(self, id, client_sock, n_next_nodes, fins_lock):   
        self.id = id
        self.client_sock = client_sock
        self.n_next_nodes = n_next_nodes
        self.fins_lock = fins_lock
        self._middleware = Middleware()  # Each child process has its own middleware connection
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal in handler so the server closes gracefully."""
        self._shutdown()
    
    def _shutdown(self):
        if self.shutting_down:
            return
        logging.info("action: Handler shutdown | result: in progress...")
        self.shutting_down = True


        self._middleware.close()
        self.client_sock.close()
        logging.info("action: Handler shutdown | result: success")

    def run(self):
        """Runs the main logic for handling a client connection."""
        while not self.shutting_down:
            try:
                raw_msg = recv_msg(self.client_sock)

                msg = decode_msg(raw_msg)

                # Process the message based on its type
                if msg.type == MsgType.CLIENT_DATA:
                    data_msg = Data( client_id=self.id, rows=msg.rows, dataset=msg.dataset)
                    self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, data_msg.encode())
                elif msg.type == MsgType.CLIENT_FIN:
                    fin_msg = SimpleMessage(type=MsgType.FIN, client_id=self.id)
                    with self.fins_lock:
                        logging.info(f"Con el lock empiezo a mandar los FINs del cliente {self.id}")
                        for _ in range(self.n_next_nodes):
                            self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, fin_msg.encode())
                        logging.info(f"Termine de mandar {self.n_next_nodes} FINs del cliente {self.id}. Suelto el lock")
                    break

            except ValueError as e:
                if not self.shutting_down:
                    logging.error(f"Connection closed or invalid message received: {e}")
                    self._shutdown()
            except OSError as e:
                if not self.shutting_down:
                    logging.error(f"action: receive_message | result: fail | error: {e}")
                    self._shutdown()
            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                    self._shutdown()