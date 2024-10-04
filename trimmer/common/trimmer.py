from messages.messages import decode_msg, MSG_TYPE_HANDSHAKE, MSG_TYPE_DATA, MSG_TYPE_FIN, GAME_CSV, REVIEW_CSV
from middleware.middleware import Middleware
import logging

GATEWAY_TRIMMER = 'gateway-trimmer'
TRIMMER_FILTERS = 'trimmer-filters'
TO_GAME = 'game'
TO_REVIEW = 'review'

class Trimmer:
    def __init__(self):
        self._middleware = Middleware()
        self._middleware.declare_queue(GATEWAY_TRIMMER)
        self._middleware.declare_exchange(TRIMMER_FILTERS, type='fanout')

    def run(self):

        while True:
            logging.warning("action: listening_queue | result: in_progress")
            raw_message = self._middleware.receive_from_queue(GATEWAY_TRIMMER)
            msg = decode_msg(raw_message[2:])
            logging.warning(f"action: listening_queue | result: success | msg: {msg}")
            if msg.type == MSG_TYPE_DATA:
                logging.warning("action: sending_data | result: in_progress")
                key = TO_GAME if msg.dataset == GAME_CSV else TO_REVIEW
                self._middleware.send_to_queue(TRIMMER_FILTERS, msg.encode(), key=key)
                logging.warning(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MSG_TYPE_FIN:
                self._middleware.send_to_queue(TRIMMER_FILTERS, msg.encode(), key=TO_GAME)
                self._middleware.send_to_queue(TRIMMER_FILTERS, msg.encode(), key=TO_REVIEW)
                self._middleware.connection.close()
                return