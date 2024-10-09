import signal
from messages.messages import Dataset, Game, Games, Genre, MsgType, Review, Score, decode_msg, Reviews
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
        games_batch = []
        reviews_batch = []
        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                # self.logger.custom("action: listening_queue | result: in_progress")
                raw_message = self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER)
                msg = decode_msg(raw_message)
                
                # Procesamos cada fila en el batch
                if msg.type == MsgType.DATA:
                    if msg.dataset == Dataset.GAME:
                        for row in msg.rows:
                            values = next(csv.reader([row]))
                            
                            game = self._get_game(values)
                            games_batch.append(game)

                        games_msg = Games(msg.id, games_batch)
                        # self.logger.custom(f"games: {games_msg}")
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, games_msg.encode(), key=K_GAME)
                        games_batch = []  # Limpiar el batch después de enviar

                    elif msg.dataset == Dataset.REVIEW:
                        for row in msg.rows:
                            values = next(csv.reader([row]))

                            # Acumula `Review` en el batch de reseñas
                            review = self._get_review(values)
                            reviews_batch.append(review)
                        
                        reviews_msg = Reviews(msg.id, reviews_batch)
                        # self.logger.custom(f"reviews: {reviews_msg}")
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, reviews_msg.encode(), key=K_REVIEW)
                        reviews_batch = []  # Limpiar el batch después de enviar

                elif msg.type == MsgType.FIN:
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GAME)
                    self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

            
    def _get_game(self, values) -> Game:
        # dataset desfasado
        app_id, name, release_date, windows, mac, linux, avg_playtime, genres = values[0], values[1], values[2], values[17], values[18], values[19], values[29], values[37]
        genres = get_genres(genres)
        game = Game(int(app_id), name, release_date, int(avg_playtime), windows == "True", linux == "True", mac == "True", genres)
        # self.logger.custom(f"Game: {game}")
        return game
    
    def _get_review(self, values):
        app_id, text, score = values[0], values[2], values[3]
        return Review(int(app_id), text, Score.from_string(score))
         