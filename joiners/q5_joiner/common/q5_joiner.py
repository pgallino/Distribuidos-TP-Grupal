from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_GENRE_Q5_JOINER = "genre-q5-joiner"
Q_SCORE_Q5_JOINER = "score-q5-joiner"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_SHOOTER_GAMES = "shooter"
K_NEGATIVE = 'negative'

class Q5Joiner:
    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q5_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)
        self._middleware.declare_queue(Q_SCORE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q5_JOINER, E_FROM_SCORE, K_NEGATIVE)

    def run(self):

        self.logger.custom("action: listen_to_queue")
        while True:
            # self.logger.custom(f'action: listening_queue: {Q_GENRE_Q5_JOINER} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_Q5_JOINER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_GENRE_Q5_JOINER} | result: success | msg: {msg}')
            if msg.type == MsgType.FIN:
                break
        
        while True:
            # self.logger.custom(f'action: listening_queue: {Q_SCORE_Q5_JOINER} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_SCORE_Q5_JOINER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_SCORE_Q5_JOINER} | result: success | msg: {msg}')
            if msg.type == MsgType.FIN:
                self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.connection.close()
                self.logger.custom("action: shutting_down | result: success")
                break