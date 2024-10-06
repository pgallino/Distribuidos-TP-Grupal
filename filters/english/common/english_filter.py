from messages.messages import decode_msg, OTHER, INDIE, SHOOTER, GAME_CSV, REVIEW_CSV, MSG_TYPE_DATA, MSG_TYPE_FIN
from middleware.middleware import Middleware
import logging

Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_POSITIVE = 'positive'
Q_ENGLISH_JOINER_Q4 = 'english-joiner_q4'

class EnglishFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_JOINER_Q4)
        self._middleware.declare_queue(Q_SCORE_ENGLISH)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_ENGLISH, E_FROM_SCORE, K_POSITIVE)

    def run(self):
        while True:
            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_SCORE_ENGLISH)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_DATA:
                self.logger.custom("action: sending_data | result: in_progress")
                self._middleware.send_to_queue(Q_ENGLISH_JOINER_Q4, msg.encode())
                self.logger.custom(f"action: sending_data | result: success | data sent to {Q_ENGLISH_JOINER_Q4}")
            elif msg.type == MSG_TYPE_FIN:
                    self._middleware.send_to_queue(Q_ENGLISH_JOINER_Q4, msg.encode())
                    self._middleware.connection.close()
                    return
