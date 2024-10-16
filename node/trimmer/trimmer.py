from messages.messages import Dataset, Genre, MsgType, decode_msg
from messages.games_msg import Q1Game, Q1Games, GenreGame, GenreGames
from messages.reviews_msg import Review, Score, Reviews
from node import Node  # Importa la clase base Nodo

from typing import List, Tuple
import csv
import sys

from utils.constants import E_COORD_TRIMMER, E_TRIMMER_FILTERS, K_GENREGAME, K_Q1GAME, K_REVIEW, Q_COORD_TRIMMER, Q_GATEWAY_TRIMMER

GAME_FIELD_NAMES = ['AppID', 'Name', 'Release date', 'Estimated owners', 'Peak CCU', 
                    'Required age', 'Price', 'Unknown', 'DiscountDLC count', 'About the game', 
                    'Supported languages', 'Full audio languages', 'Reviews', 'Header image', 
                    'Website', 'Support url', 'Support email', 'Windows', 'Mac', 
                    'Linux', 'Metacritic score', 'Metacritic url', 'User score', 
                    'Positive', 'Negative', 'Score rank', 'Achievements', 
                    'Recommendations', 'Notes', 'Average playtime forever', 
                    'Average playtime two weeks', 'Median playtime forever', 
                    'Median playtime two weeks', 'Developers', 'Publishers', 
                    'Categories', 'Genres', 'Tags', 'Screenshots', 'Movies']

REVIEW_FIELD_NAMES = ['app_id','app_name','review_text','review_score','review_votes']

# Aumenta el límite del tamaño de campo
csv.field_size_limit(sys.maxsize)  # Esto establece el límite en el tamaño máximo permitido por el sistema

def get_genres(genres_string: str):
    values = genres_string.split(',')
    return [genre for value in values if (genre := Genre.from_string(value)) != Genre.OTHER]

class Trimmer(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        super().__init__(id, n_nodes, n_next_nodes)

        # Configura las colas y los intercambios específicos para Trimmer
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        
        self._setup_coordination_queue(Q_COORD_TRIMMER, E_COORD_TRIMMER)


    def run(self):

        try:

            self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER, self._process_message, auto_ack=False)

            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)
            
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        keys = []
        for node, n_nodes in self.n_next_nodes:
            if node == 'GENRE':
                keys.append((K_GENREGAME, n_nodes))
            if node == 'SCORE':
                keys.append((K_REVIEW, n_nodes))
            if node == 'OS_COUNTER':
                keys.append((K_Q1GAME, n_nodes))
                
        self._process_fin(raw_message, keys, E_TRIMMER_FILTERS)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_message(self, ch, method, properties, raw_message):

        """Callback para procesar mensajes de la cola"""

        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.DATA:
            self._process_data_message(msg)

        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_data_message(self, msg):
        """Procesa mensajes de tipo DATA, filtrando juegos y reseñas."""
        genre_games_batch, q1_games_batch, reviews_batch = [], [], []
        
        if msg.dataset == Dataset.GAME:
            self._process_game_data(msg, genre_games_batch, q1_games_batch)
        elif msg.dataset == Dataset.REVIEW:
            self._process_review_data(msg, reviews_batch)

    def _process_game_data(self, msg, genre_games_batch, q1_games_batch):
        """Procesa datos GAME y envía a las colas correspondientes."""
        reader = csv.DictReader(msg.rows, fieldnames=GAME_FIELD_NAMES)
        for values in reader:
            q1_game, genre_game = self._get_game(values)
            if q1_game:
                q1_games_batch.append(q1_game)
            if genre_game:
                genre_games_batch.append(genre_game)

        # Enviar lotes por separado para cada tipo de juego
        if q1_games_batch:
            q1_games_msg = Q1Games(msg.id, q1_games_batch)
            self._middleware.send_to_queue(E_TRIMMER_FILTERS, q1_games_msg.encode(), key=K_Q1GAME)
        if genre_games_batch:
            genre_games_msg = GenreGames(msg.id, genre_games_batch)
            self._middleware.send_to_queue(E_TRIMMER_FILTERS, genre_games_msg.encode(), key=K_GENREGAME)

    def _process_review_data(self, msg, reviews_batch):
        """Procesa datos del dataset REVIEW y envía a la cola correspondiente."""
        reader = csv.DictReader(msg.rows, fieldnames=REVIEW_FIELD_NAMES)
        for values in reader:
            review = self._get_review(values)
            if review:
                reviews_batch.append(review)
        
        if reviews_batch:
            reviews_msg = Reviews(msg.id, reviews_batch)
            self._middleware.send_to_queue(E_TRIMMER_FILTERS, reviews_msg.encode(), key=K_REVIEW)

    def _process_fin_message(self, msg):
        """Procesa mensajes de tipo FIN, coordinando con otros nodos si es necesario."""

        # deja de consumir de la cola original para no obtener más FIN si hay
        self._middleware.channel.stop_consuming()

        if self.n_nodes > 1:
            self._middleware.send_to_queue(E_COORD_TRIMMER, msg.encode(), key=f"coordination_{self.id}")
        else:
            for node, n_nodes in self.n_next_nodes:
                for _ in range(n_nodes):
                    if node == 'GENRE':
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GENREGAME)
                    elif node == 'SCORE':
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                    elif node == 'OS_COUNTER':
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_Q1GAME)

    def _get_game(self, values):
        """
        Crea una instancia de Q1Game y/o GenreGame a partir de los datos, descartando aquellas filas con valores vacíos.
        """
        # Claves necesarias
        required_keys = ['AppID', 'Name', 'Windows', 'Mac', 'Linux', 'Genres', 'Release date', 'Average playtime forever', 'Positive', 'Negative']
        
        # Verificar si alguno de los valores críticos está ausente o es una cadena vacía
        for key in required_keys:
            if key not in values or values[key].strip() == "":
                # print(f"Valor faltante o vacío para la clave: {key}")
                return None, None

        try:
            app_id = int(values['AppID'])
            name = values['Name']
            release_date = values['Release date']
            avg_playtime = int(values['Average playtime forever'])
            windows = values['Windows'] == "True"
            mac = values['Mac'] == "True"
            linux = values['Linux'] == "True"
            genres = get_genres(values['Genres'])
        except (ValueError, KeyError) as e:
            print(f"Error al procesar los valores en _get_game: {e}")
            return None, None

        # Crear Q1Game con compatibilidad de plataformas
        q1_game = Q1Game(app_id, windows, linux, mac) if any([windows, linux, mac]) else None

        # Crear GenreGame si hay géneros y otros detalles
        genre_game = GenreGame(app_id, name, release_date, avg_playtime, genres) if genres else None

        return q1_game, genre_game


    def _get_review(self, values):
        required_keys = ['app_id', 'review_text', 'review_score']
        for key in required_keys:
            if key not in values or values[key] == "":
                # app_id = int(values["app_id"])
                # score = Score.from_string(values['review_score'])
                # if (app_id == 105600 or app_id == 252950 or app_id == 391540) and (score == Score.POSITIVE):
                    # print(f"Valor faltante o vacío para la clave en _get_review: {key}. Review de juego {app_id} con valor: {values['review_text']}")
                return None

        try:
            app_id = int(values['app_id'])
            text = values['review_text']
            score = Score.from_string(values['review_score'])
        except (ValueError, KeyError) as e:
            print(f"Error al procesar la reseña en _get_review: {e}")
            return None

        return Review(app_id, text, score)

            