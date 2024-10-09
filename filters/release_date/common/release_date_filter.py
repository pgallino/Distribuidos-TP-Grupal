import signal
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_GENRE_RELEASE_DATE = "genre-release_date"
E_FROM_GENRE = 'from_genre'
K_INDIE_GAMES = 'indie'
Q_2010_GAMES = '2010_games'

class ReleaseDateFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_GAMES)
        self._middleware.declare_queue(Q_2010_GAMES)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                # # self.logger.custom('action: listening_queue | result: in_progress')
                raw_message = self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE)
                msg = decode_msg(raw_message[4:])
                # # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
                
                if msg.type == MsgType.GAME and "201" in msg.release_date:
                    self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                    # # self.logger.custom(f"action: sending_data | result: success | data sent to {Q_2010_GAMES}")
                
                if msg.type == MsgType.FIN:
                    # Se reenvia el FIN al resto de los nodos
                    # # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                    self._middleware.connection.close()
                    # # self.logger.custom("action: shutting_down | result: success")
                    return
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")