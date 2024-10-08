from messages.messages import decode_msg
from middleware.middleware import Middleware
import logging

Q_GENRE_RELEASE_DATE = "genre-release_date"
E_FROM_GENRE = 'from_genre'
K_INDIE_GAMES = 'indie'

class ReleaseDateFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_GAMES)

    def run(self):
        while True:
            self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE)
            self.logger.custom(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')