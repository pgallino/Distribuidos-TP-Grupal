from messages.messages import Dataset, Game, Genre, MsgType, Review, Score, decode_msg, MSG_TYPE_HANDSHAKE, MSG_TYPE_DATA, MSG_TYPE_FIN, GAME_CSV, REVIEW_CSV
from middleware.middleware import Middleware
import logging
from distutils.util import strtobool

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
E_TRIMMER_FILTERS = 'trimmer-filters'
K_GAME = 'game'
K_REVIEW = 'review'


#### ESTANDAR NOMBRE COLAS ####
# Q_ORIGEN_DESTINO = "origen-destino"

#### ESTANDAR NOMBRE EXCHANGES ####
# E_FROM_ORIGEN = "from_origen"

#### ESTANDAR NOMBRE CLAVES ####
# K_GAME = "game"

def get_genres(genres_string: str):
    values = genres_string.split(',')
    return [Genre.from_string(value) for value in values]

class Trimmer:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)

    def run(self):

        while True:
            self.logger.custom("action: listening_queue | result: in_progress")
            raw_message = self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")
            if MsgType(msg.type) == MsgType.DATA:
                self.logger.custom("action: sending_data | result: in_progress")
                values = msg.row.split(',')
                if msg.dataset == Dataset.GAME:
                    next_msg = self._get_game(msg.id, values).encode()
                else:
                    next_msg = self._get_review(msg.id, values).encode()
                key = K_GAME if msg.dataset == GAME_CSV else K_REVIEW
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, next_msg.encode(), key=key)
                self.logger.custom(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MSG_TYPE_FIN:
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GAME)
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                    self._middleware.connection.close()
                    return
            
    def _get_game(self, client_id, values) -> Game:
        app_id, name, release_date, windows, mac, linux, avg_playtime, genres = values[0], values[1], values[2], values[16], values[17], values[18], values[32], values[39]
        genres = get_genres(genres)
        return Game(client_id, int(app_id), name, release_date, (avg_playtime), windows == "True", linux == "True", mac == "True", genres)
    
    def _get_review(self, client_id, values):
        app_id, text, score = values[0], values[2], values[3]
        return Review(client_id, app_id, text, Score.from_string(score))
         