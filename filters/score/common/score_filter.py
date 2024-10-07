from messages.messages import MsgType, Score, decode_msg
from middleware.middleware import Middleware
import logging

Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_REVIEW = 'review'
Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_POSITIVE = 'positive'
K_NEGATIVE = 'negative'

class ScoreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_TRIMMER_FILTERS, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)

    def run(self):
        self.logger.custom("action: listen_to_queue")
        while True:
            # self.logger.custom("action: listening_queue | result: in_progress")
            raw_message = self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER)
            msg = decode_msg(raw_message[2:])
            # self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")
            if msg.type == MsgType.REVIEW:
                # self.logger.custom("action: sending_data | result: in_progress")
                key = K_POSITIVE if msg.score == Score.POSITIVE else K_NEGATIVE
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=key)
                # self.logger.custom(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MsgType.FIN:
                # Se reenvia el FIN al resto de nodos
                self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_POSITIVE)
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_NEGATIVE)
                self._middleware.connection.close()
                self.logger.custom("action: shutting_down | result: success")
                return