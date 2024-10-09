from messages.messages import Dataset, Genre, MsgType, Review, Score, decode_msg, Reviews, Q1Game, GenreGame, Q1Games, GenreGames
from middleware.middleware import Middleware
import logging
import csv
import sys

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
E_TRIMMER_FILTERS = 'trimmer-filters'
K_Q1GAME = 'q1game'
K_GENREGAME = 'genregame'
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
    return [genre for value in values if (genre := Genre.from_string(value)) != Genre.OTHER]

class Trimmer:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)

    def run(self):
        genre_games_batch = []
        q1_games_batch = []
        reviews_batch = []
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
                        
                        # Crear instancias de Q1Game y GenreGame
                        q1_game, genre_game = self._get_game(values)
                        if q1_game:
                            q1_games_batch.append(q1_game)
                        if genre_game:
                            genre_games_batch.append(genre_game)

                    # Enviar lotes por separado para cada tipo de juego
                    if q1_games_batch:
                        q1_games_msg = Q1Games(msg.id, q1_games_batch)
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, q1_games_msg.encode(), key=K_Q1GAME)
                        q1_games_batch = []

                    if genre_games_batch:
                        genre_games_msg = GenreGames(msg.id, genre_games_batch)
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, genre_games_msg.encode(), key=K_GENREGAME)
                        genre_games_batch = []

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
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GENREGAME)
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_Q1GAME)
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                self._middleware.connection.close()
                # self.logger.custom("action: shutting_down | result: success")
                return

            
    def _get_game(self, values):
        """
        Crea una instancia de Q1Game y/o GenreGame a partir de los datos.
        """
        app_id = int(values[0])
        name = values[1]
        release_date = values[2]
        avg_playtime = int(values[29])
        windows = values[17] == "True"
        mac = values[18] == "True"
        linux = values[19] == "True"
        genres = get_genres(values[37])

        # Crear Q1Game con compatibilidad de plataformas
        q1_game = Q1Game(app_id, windows, linux, mac) if any([windows, linux, mac]) else None

        # Crear GenreGame si hay géneros y otros detalles
        genre_game = GenreGame(app_id, name, release_date, avg_playtime, genres) if genres else None

        return q1_game, genre_game
    
    def _get_review(self, values):
        app_id, text, score = values[0], values[2], values[3]
        return Review(int(app_id), text, Score.from_string(score))
         