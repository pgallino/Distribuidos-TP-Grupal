from messages.messages import decode_msg, MSG_TYPE_HANDSHAKE, MSG_TYPE_DATA, MSG_TYPE_FIN, GAME_CSV, REVIEW_CSV
from middleware.middleware import Middleware
import logging

Q_ENGLISH_JOINER = 'english-joiner'

class JoinerQ4:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_JOINER)

    def run(self):

        while True:
            self.logger.custom("action: listening_queue | result: in_progress")
            raw_message = self._middleware.receive_from_queue(Q_ENGLISH_JOINER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")