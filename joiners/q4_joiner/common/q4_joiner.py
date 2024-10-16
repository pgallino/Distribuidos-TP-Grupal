from collections import defaultdict
import signal
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q4Result
from messages.reviews_msg import TextReview, TextReviews
from node.node import Node

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
        self.negative_review_counts = defaultdict(int)  # Contará reseñas negativas en inglés
        self.games = {}  # Detalles de juegos de acción/shooter
        self.negative_reviews = {} # Guarda las reviews negativas de los juegos

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q4_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game
                
        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_ENGLISH_Q4_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews: # para un TextReview en TextReviews
                if review.app_id in self.games:
                    # Debe funcionar appendiendo el elemento directamente en el get tambien
                    curr_negative_reviews = self.negative_reviews.get(review.app_id, [])
                    curr_negative_reviews.append(review.text)
                    self.negative_reviews[review.app_id] = curr_negative_reviews

        elif msg.type == MsgType.FIN:

            # deja de consumir
            self._middleware.channel.stop_consuming()

            # Revisa que juego tiene mas de 5000 resenia negativas
            for app_id, reviews in self.negative_reviews.items():
                if len(reviews) > self.n_reviews:
                    # manda las reviews del juego al filtro de ingles
                    reviews_batch = []
                    curr_reviews_batch_size = 0
                    for review in reviews:
                        text_review = TextReview(app_id, review)
                        text_review_size = len(text_review.encode())
                        if text_review_size + curr_reviews_batch_size > self.batch_size:
                            text_reviews = TextReviews(msg.id, reviews_batch)
                            self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
                            curr_reviews_batch_size = 0
                            reviews_batch = []
                        curr_reviews_batch_size += text_review_size
                        reviews_batch.append(text_review)

                    # si me quedaron afuera    
                    if reviews_batch:
                        text_reviews = TextReviews(msg.id, reviews_batch)
                        self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
            
            self.negative_reviews.clear() #limpio el diccionario

            # Manda el fin a los english filters
            for _, n_nodes in self.n_next_nodes:
                for _ in range(n_nodes):
                    self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, msg.encode())

        ch.basic_ack(delivery_tag=method.delivery_tag)
                            
    def process_negative_review_message(self, ch, method, properties, raw_message):

        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews: # para un TextReview en TextReviews
                if review.app_id in self.games:
                    self.negative_review_counts[review.app_id] += 1

        elif msg.type == MsgType.FIN:

            self._middleware.channel.stop_consuming()

            # Filtrar juegos de acción con más de 5,000 reseñas negativas en inglés
            negative_reviews = sorted(
                [
                    (app_id, self.games[app_id].name, count)
                    for app_id, count in self.negative_review_counts.items()
                    if count > self.n_reviews
                ],
                key=lambda x: x[0],  # Ordenar por app_id
                reverse=False  # Orden descendente
            )[:25]  # Tomar los 25 primeros


            # Crear y enviar el mensaje Q4Result
            result_message = Q4Result(id=msg.id, negative_reviews=negative_reviews)
            self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            self._middleware.receive_from_queue(Q_GENRE_Q4_JOINER, self.process_game_message, auto_ack=False)
            self._middleware.receive_from_queue(Q_SCORE_Q4_JOINER, self.process_review_message, auto_ack=False)
            self._middleware.receive_from_queue(Q_ENGLISH_Q4_JOINER, self.process_negative_review_message, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self._shutdown()