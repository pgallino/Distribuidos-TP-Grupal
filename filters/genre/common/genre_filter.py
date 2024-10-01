from messages.messages import decode_msg
from middleware.middleware import Middleware
import logging

class GenreFilter:

    def __init__(self):
        self._middleware = Middleware()
        self._middleware.declare_queue("general_queue")

    def run(self):
        while True:
            logging.info('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue("general_queue")
            logging.info(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')