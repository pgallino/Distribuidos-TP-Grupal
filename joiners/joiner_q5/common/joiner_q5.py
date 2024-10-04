from messages.messages import decode_msg
from middleware.middleware import Middleware
import logging

Q_GENRE_JOINER_Q5 = "genre-joiner-q5"
Q_SCORE_JOINER_Q5 = "score-joiner-q5"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_SHOOTER_GAMES = "shooter"
K_NEGATIVE = 'negative'

class JoinerQ5:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()

        self._middleware.declare_queue(Q_GENRE_JOINER_Q5)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_JOINER_Q5, E_FROM_GENRE, K_SHOOTER_GAMES)

        self._middleware.declare_queue(Q_SCORE_JOINER_Q5)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_JOINER_Q5, E_FROM_SCORE, K_NEGATIVE)

    def run(self):
        while True:
            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_JOINER_Q5)
            self.logger.custom(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')

            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_SCORE_JOINER_Q5)
            self.logger.custom(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')