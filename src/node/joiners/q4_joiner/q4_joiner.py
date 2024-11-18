from collections import defaultdict
import logging
from typing import List, Tuple
from messages.messages import MsgType, ResultMessage, TextReviewsMessage, decode_msg
from messages.results_msg import Q4Result
from messages.reviews_msg import TextReview
from node import Node

from utils.constants import E_FROM_SCORE, K_NEGATIVE_TEXT, Q_SCORE_Q4_JOINER, Q_Q4_JOINER_ENGLISH, E_FROM_GENRE, K_SHOOTER_GAMES, Q_ENGLISH_Q4_JOINER, Q_GENRE_Q4_JOINER, Q_QUERY_RESULT_4

class Q4Joiner(Node):

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], batch_size: int, n_reviews: int):
        super().__init__(id, n_nodes, n_next_nodes)

        self.batch_size = batch_size * 1024
        self.n_reviews = n_reviews
        
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_queue(Q_SCORE_Q4_JOINER)
        self._middleware.declare_queue(Q_Q4_JOINER_ENGLISH)
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)
        self._middleware.bind_queue(Q_SCORE_Q4_JOINER, E_FROM_SCORE, K_NEGATIVE_TEXT)

        # Estructuras de almacenamiento
        self.negative_review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Contará reseñas negativas en inglés, para cada cliente
        self.games_per_client = defaultdict(lambda: {})  # Detalles de juegos de acción/shooter
        self.negative_reviews_per_client = defaultdict(lambda: defaultdict(lambda: ([], False))) # Guarda las reviews negativas de los juegos
        self.fins_per_client = defaultdict(lambda: [False, False]) #primer valor corresponde al fin de juegos, y el segundo al de reviews


    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q4_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            client_games = self.games_per_client[msg.id]
            for game in msg.games:
                client_games[game.app_id] = game
                
        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[0] = True
            if client_fins[0] and client_fins[1]:
                self.send_reviews(msg.id)
                # Manda el fin a los english filters
                self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, msg.encode())
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_ENGLISH_Q4_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            client_reviews = self.negative_reviews_per_client[msg.id]
            client_games = self.games_per_client[msg.id]
            games_fin_received = self.fins_per_client[msg.id][0]
            for review in msg.reviews: # para un TextReview en TextReviews
                if (not games_fin_received) or review.app_id in client_games:
                    # Debe funcionar appendiendo el elemento directamente de esta manera
                    game_reviews, _ = client_reviews[review.app_id]
                    game_reviews.append(review.text)
                    if len(game_reviews) > self.n_reviews:
                        self.send_reviews_v2(msg.id, review.app_id, game_reviews)
                        client_reviews[review.app_id] = ([], True)

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[1] = True
            if client_fins[0] and client_fins[1]:
                self.send_reviews(msg.id)
                # Manda el fin a los english filters
                self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, msg.encode())

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_reviews_v2(self, client_id, app_id, reviews):
        # manda las reviews del juego al filtro de ingles
        reviews_batch = []
        curr_reviews_batch_size = 0

        for review in reviews:
            text_review = TextReview(app_id, review)
            text_review_size = len(text_review.encode())
            if text_review_size + curr_reviews_batch_size > self.batch_size:
                text_reviews = TextReviewsMessage(id=client_id, reviews=reviews_batch)
                self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
                curr_reviews_batch_size = 0
                reviews_batch = []
            curr_reviews_batch_size += text_review_size
            reviews_batch.append(text_review)

        # si me quedaron afuera    
        if reviews_batch:
            text_reviews = TextReviewsMessage(id=client_id, reviews=reviews_batch)
            self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())

    def send_reviews(self, client_id):
        client_reviews = self.negative_reviews_per_client[client_id]

        # Revisa que juego tiene mas de 5000 resenia negativas
        for app_id, (reviews, overpass_threshold) in client_reviews.items():
            if overpass_threshold or len(reviews) > self.n_reviews:
                # manda las reviews del juego al filtro de ingles
                reviews_batch = []
                curr_reviews_batch_size = 0
                for review in reviews:
                    text_review = TextReview(app_id, review)
                    text_review_size = len(text_review.encode())
                    if text_review_size + curr_reviews_batch_size > self.batch_size:
                        text_reviews = TextReviewsMessage(id=client_id, reviews=reviews_batch)
                        self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
                        curr_reviews_batch_size = 0
                        reviews_batch = []
                    curr_reviews_batch_size += text_review_size
                    reviews_batch.append(text_review)

                # si me quedaron afuera    
                if reviews_batch:
                    text_reviews = TextReviewsMessage(id=client_id, reviews=reviews_batch)
                    self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())

        # Borro el diccionario de textos de reviews del cliente
        del self.negative_reviews_per_client[client_id]
        
        client_reviews.clear() #limpio el diccionario

                            
    def process_negative_review_message(self, ch, method, properties, raw_message):

        msg = decode_msg(raw_message)

        client_reviews_count = self.negative_review_counts_per_client[msg.id]
        client_games = self.games_per_client[msg.id]

        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews: # para un TextReview en TextReviews
                if review.app_id in client_games:
                    client_reviews_count[review.app_id] += 1

        elif msg.type == MsgType.FIN:
            # Filtrar juegos de acción con más de 5,000 reseñas negativas en inglés
            negative_reviews = sorted(
                [
                    (app_id, client_games[app_id].name, count)
                    for app_id, count in client_reviews_count.items()
                    if count > self.n_reviews
                ],
                key=lambda x: x[0],  # Ordenar por app_id
                reverse=False  # Orden descendente
            )[:25]  # Tomar los 25 primeros


            # Crear y enviar el mensaje Q4Result
            q4_result = Q4Result(negative_reviews=negative_reviews)
            result_message = ResultMessage(id=msg.id, result=q4_result)
            self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())

            # Borro los diccionarios de clientes ya resueltos
            del self.games_per_client[msg.id]
            del self.negative_review_counts_per_client[msg.id]

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q4_JOINER, self.process_game_message), (Q_SCORE_Q4_JOINER, self.process_review_message), (Q_ENGLISH_Q4_JOINER, self.process_negative_review_message)], auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()