from messages.messages import decode_msg
from middleware.middleware import Middleware
import logging

GENRE_RELEASE_DATE = "genre-release_date"
FROM_GENRE = 'from_genre'
INDIE_GAMES = 'indie'

class ReleaseDateFilter:

    def __init__(self):
        self._middleware = Middleware()
        self._middleware.declare_queue(GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(FROM_GENRE)
        self._middleware.bind_queue(GENRE_RELEASE_DATE, FROM_GENRE, INDIE_GAMES)

    def run(self):
        while True:
            logging.warning('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(GENRE_RELEASE_DATE)
            logging.warning(f'action: listening_queue | result: success | msg: {decode_msg(raw_message[2:])}')