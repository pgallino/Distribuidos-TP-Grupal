import logging
import signal
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
from utils.constants import Q_GATEWAY_TRIMMER
from utils.utils import recv_msg


class ConnectionHandler:
    """Handles communication with a connected client in a separate process."""

    def __init__(self, id, client_sock, n_next_nodes):
        self.id = id
        self.client_sock = client_sock
        self.n_next_nodes = n_next_nodes
        self._middleware = Middleware()  # Each child process has its own middleware connection
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal in handler so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self._shutdown()
    
    def _shutdown(self):
        if self.shutting_down:
            return
        self.logger.custom("action: shutdown | result: in progress...")
        self.shutting_down = True


        self._middleware.close()
        self.client_sock.close()
        self.logger.custom("action: shutdown | result: success")

    def run(self):
        """Runs the main logic for handling a client connection."""
        while not self.shutting_down:
            try:
                raw_msg = recv_msg(self.client_sock)

                msg = decode_msg(raw_msg)
                msg.id = self.id  # Cambia el id de los mensajes

                # Process the message based on its type
                if msg.type == MsgType.DATA:
                    self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, msg.encode())
                elif msg.type == MsgType.FIN:
                    self._middleware.send_to_queue(Q_GATEWAY_TRIMMER, msg.encode())
                    break

            except ValueError as e:
                if not self.shutting_down:
                    self.logger.error(f"Connection closed or invalid message received: {e}")
                    self._shutdown()
            except OSError as e:
                if not self.shutting_down:
                    self.logger.error(f"action: receive_message | result: fail | error: {e}")
                    self._shutdown()
            except Exception as e:
                if not self.shutting_down:
                    self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                    self._shutdown()