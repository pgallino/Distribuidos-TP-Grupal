from messages.messages import decode_msg, MSG_TYPE_FIN
from middleware.middleware import Middleware
import logging

Q_GENRE_JOINER_Q3 = "genre-joiner-q3"
Q_SCORE_JOINER_Q3 = "score-joiner-q3"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_INDIE_GAMES = "indie"
K_POSITIVE = 'positive'

class Q3Joiner:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_JOINER_Q3)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_JOINER_Q3, E_FROM_GENRE, K_INDIE_GAMES)

        self._middleware.declare_queue(Q_SCORE_JOINER_Q3)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_JOINER_Q3, E_FROM_SCORE, K_POSITIVE)

    def run(self):
        while True:
            self.logger.custom(f'action: listening_queue: {Q_GENRE_JOINER_Q3} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_JOINER_Q3)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_GENRE_JOINER_Q3} | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_FIN:
                break
        
        while True:
            self.logger.custom(f'action: listening_queue: {Q_SCORE_JOINER_Q3} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_SCORE_JOINER_Q3)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_SCORE_JOINER_Q3} | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_FIN:
                break