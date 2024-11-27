import logging
from messages.messages import Dataset, ListMessage, MsgType, decode_msg, NodeType
from messages.games_msg import GamesType, Q1Game, GenreGame, Genre
from messages.reviews_msg import Review, ReviewsType, Score
from node import Node  # Importa la clase base Nodo

from typing import List, Tuple
import csv
import sys

from utils.constants import E_COORD_TRIMMER, E_FROM_TRIMMER, K_GENREGAME, K_Q1GAME, K_REVIEW, Q_COORD_TRIMMER, Q_GATEWAY_TRIMMER

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
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], container_name: str):
        super().__init__(id, n_nodes, n_next_nodes, container_name=container_name)

        # Configura las colas y los intercambios específicos para Trimmer
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        if self.n_nodes > 1: self._middleware.declare_exchange(E_COORD_TRIMMER)

    def get_keys(self):
        keys = []
        for node, n_nodes in self.n_next_nodes:
            if node == 'genre':
                keys.append((K_GENREGAME, n_nodes))
            elif node == 'score':
                keys.append((K_REVIEW, n_nodes))
            elif node == 'os_counter':
                keys.append((K_Q1GAME, n_nodes))
        return keys
    
    def get_type(self):
        return NodeType.TRIMMER
        
    def run(self):

        try:
            self.init_ka(self.container_name)
            if self.n_nodes > 1:
                self.init_coordinator(self.id, Q_COORD_TRIMMER, E_COORD_TRIMMER, self.n_nodes, self.get_keys(), E_FROM_TRIMMER)
            
            self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER, self._process_message, auto_ack=False)
            
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()


    def _process_message(self, ch, method, properties, raw_message):

        """Callback para procesar mensajes de la cola"""

        msg = decode_msg(raw_message)

        with self.condition:
            self.processing_client.value = msg.client_id # SETEO EL ID EN EL processing_client -> O SEA ESTOY PROCESANDO UN MENSAJE DE CLIENTE ID X
            self.condition.notify_all()
        
        if msg.type == MsgType.DATA:
            self._process_data_message(msg)

        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        with self.condition:
            self.processing_client.value = -1 # SETEO EL -1 EN EL processing_client -> TERMINE DE PROCESAR
            self.condition.notify_all()

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
            q1_games_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.Q1GAMES, items=q1_games_batch, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_TRIMMER, q1_games_msg.encode(), key=K_Q1GAME)
        if genre_games_batch:
            genre_games_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.GENREGAMES, items=genre_games_batch, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_TRIMMER, genre_games_msg.encode(), key=K_GENREGAME)

    def _process_review_data(self, msg, reviews_batch):
        """Procesa datos del dataset REVIEW y envía a la cola correspondiente."""
        reader = csv.DictReader(msg.rows, fieldnames=REVIEW_FIELD_NAMES)
        for values in reader:
            review = self._get_review(values)
            if review:
                reviews_batch.append(review)
        
        if reviews_batch:
            reviews_msg = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.FULLREVIEW, items=reviews_batch, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_TRIMMER, reviews_msg.encode(), key=K_REVIEW)

    def _process_fin_message(self, msg):
        """Procesa mensajes de tipo FIN, coordinando con otros nodos si es necesario."""

        # Ya no dejo de consumir

        if self.n_nodes > 1:
            self.forward_coordfin(E_COORD_TRIMMER, msg)
        else:
            for node, _ in self.n_next_nodes:
                if node == 'genre':
                    self._middleware.send_to_queue(E_FROM_TRIMMER, msg.encode(), key=K_GENREGAME)
                elif node == 'score':
                    self._middleware.send_to_queue(E_FROM_TRIMMER, msg.encode(), key=K_REVIEW)
                elif node == 'os_counter':
                    self._middleware.send_to_queue(E_FROM_TRIMMER, msg.encode(), key=K_Q1GAME)

    def _get_game(self, values):
        """
        Crea una instancia de Q1Game y/o GenreGame a partir de los datos, descartando aquellas filas con valores vacíos.
        """
        # Claves necesarias
        required_keys = ['AppID', 'Name', 'Windows', 'Mac', 'Linux', 'Genres', 'Release date', 'Average playtime forever', 'Positive', 'Negative']
        
        # Verificar si alguno de los valores críticos está ausente o es una cadena vacía
        for key in required_keys:
            if key not in values or values[key].strip() == "":
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
                return None

        try:
            app_id = int(values['app_id'])
            text = values['review_text']
            score = Score.from_string(values['review_score'])
        except (ValueError, KeyError) as e:
            return None

        return Review(app_id, text, score)

            