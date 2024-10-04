from messages.messages import decode_msg, OTHER, INDIE, SHOOTER, GAME_CSV, REVIEW_CSV, MSG_TYPE_DATA, MSG_TYPE_FIN
from middleware.middleware import Middleware
import logging

Q_TRIMMER_GENRE_FILTER = "trimmer-genre_filter"
E_FROM_TRIMMER = 'trimmer-filters'
K_GAME = 'game'
E_FROM_GENRE = 'from_genre'
K_INDIE_GAMES = 'indie'
K_SHOOTER_GAMES = 'shooter'

class GenreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

    def run(self):
        while True:
            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_DATA:
                if msg.genre != OTHER:
                    key = K_INDIE_GAMES if msg.genre == INDIE  else K_SHOOTER_GAMES
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=key)
                    self.logger.custom(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MSG_TYPE_FIN:
                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_GAMES)
                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)
                # mandar al resto de nodos
                self._middleware.connection.close()
                return