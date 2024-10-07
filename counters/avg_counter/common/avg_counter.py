from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_2010_GAMES = '2010_games'

class AvgCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_2010_GAMES)

    def run(self):
        while True:
            # self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_2010_GAMES)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            if msg.type == MsgType.FIN:
                self._middleware.connection.close()
                return