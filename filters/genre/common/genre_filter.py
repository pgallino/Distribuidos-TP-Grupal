from messages.messages import decode_msg, OTHER, INDIE, SHOOTER, GAME_CSV, REVIEW_CSV, MSG_TYPE_DATA, MSG_TYPE_FIN
from middleware.middleware import Middleware
import logging

TRIMMER_GENRE_FILTER = "trimmer-genre_filter"
TRIMMER_FILTERS = 'trimmer-filters'
TO_GAME = 'game'
FROM_GENRE = 'from_genre'
INDIE_GAMES = 'indie'
SHOOTER_GAMES = 'shooter'

class GenreFilter:

    def __init__(self):
        self._middleware = Middleware()
        self._middleware.declare_queue(TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(TRIMMER_FILTERS, type='fanout')
        self._middleware.bind_queue(TRIMMER_GENRE_FILTER, TRIMMER_FILTERS, TO_GAME)
        self._middleware.declare_exchange(FROM_GENRE)

    def run(self):
        while True:
            logging.warning('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(TRIMMER_GENRE_FILTER)
            msg = decode_msg(raw_message[2:])
            logging.warning(f'action: listening_queue | result: success | msg: {msg}')
            if msg.type == MSG_TYPE_DATA:
                if msg.genre != OTHER:
                    key = INDIE_GAMES if msg.genre == INDIE  else SHOOTER_GAMES
                    self._middleware.send_to_queue(TRIMMER_FILTERS, msg.encode(), key=key)
                    logging.warning(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MSG_TYPE_FIN:
                self._middleware.send_to_queue(TRIMMER_FILTERS, msg.encode(), key=INDIE_GAMES)
                # mandar al resto de nodos
                self._middleware.connection.close()
                return