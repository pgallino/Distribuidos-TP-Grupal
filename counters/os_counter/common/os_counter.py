from messages.messages import MSG_TYPE_FIN, decode_msg
from middleware.middleware import Middleware
import logging

Q_TRIMMER_OS_COUNTER = "trimmer-os_counter"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_GAME = 'game'

class OsCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_TRIMMER_FILTERS, K_GAME)

    def run(self):
        while True:
            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_FIN:
                self._middleware.connection.close()
                return