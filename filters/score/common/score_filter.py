from messages.messages import decode_msg
from middleware.middleware import Middleware
import logging

TRIMMER_SCORE_FILTER = "trimmer-score_filter"
TRIMMER_FILTERS = 'trimmer-filters'
TO_REVIEW = 'review'

class ScoreFilter:

    def __init__(self):
        self._middleware = Middleware()
        self._middleware.declare_queue(TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(TRIMMER_FILTERS)
        self._middleware.bind_queue(TRIMMER_SCORE_FILTER, TRIMMER_FILTERS, TO_REVIEW)

    def run(self):
        while True:
            logging.warning('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(TRIMMER_SCORE_FILTER)
            logging.warning(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')