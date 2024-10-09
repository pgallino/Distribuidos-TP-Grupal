import signal
from messages.messages import Dataset, Game, Genre, MsgType, Review, Score, decode_msg
from middleware.middleware import Middleware
import logging
import csv
import sys

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
E_TRIMMER_FILTERS = 'trimmer-filters'
K_GAME = 'game'
K_REVIEW = 'review'

# Aumenta el límite del tamaño de campo
csv.field_size_limit(sys.maxsize)  # Esto establece el límite en el tamaño máximo permitido por el sistema


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
        self.shutting_down = False
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                # self.logger.custom("action: listening_queue | result: in_progress")
                raw_message = self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER)
                msg = decode_msg(raw_message[4:])
                # self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")
                if msg.type == MsgType.DATA:
                    # self.logger.custom("action: sending_data | result: in_progress")
                    values = next(csv.reader([msg.row]))
                    # self.logger.custom(f"values: {values}")
                    if msg.dataset == Dataset.GAME:
                        next_msg = self._get_game(msg.id, values)
                        key = K_GAME
                    else:
                        next_msg = self._get_review(msg.id, values)
                        key = K_REVIEW
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, next_msg.encode(), key=key)
                    # self.logger.custom(f"action: sending_data | result: success | data sent to {key}")
                elif msg.type == MsgType.FIN:
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GAME)
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
            
            
    def _get_game(self, client_id, values) -> Game:
        app_id, name, release_date, windows, mac, linux, avg_playtime, genres = values[0], values[1], values[2], values[16], values[17], values[18], values[28], values[35]
        genres = get_genres(genres)
        return Game(client_id, int(app_id), name, release_date, int(avg_playtime), windows == "True", linux == "True", mac == "True", genres)
    
    def _get_review(self, client_id, values):
        app_id, text, score = values[0], values[2], values[3]
        return Review(client_id, int(app_id), text, Score.from_string(score))
         