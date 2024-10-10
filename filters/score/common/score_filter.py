import signal
from typing import List, Tuple
from messages.messages import BasicReview, BasicReviews, MsgType, Score, TextReview, TextReviews, decode_msg, BATCH_SIZE, Reviews, Review
from middleware.middleware import Middleware
import logging

Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_REVIEW = 'review'
Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
E_COORD_SCORE = 'from-coord-score'
K_NEGATIVE = 'negative'
K_NEGATIVE_TEXT = 'negative_text'
K_POSITIVE = 'positive'
Q_COORD_SCORE = 'coord-score'

class ScoreFilter:

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):

        self.id = id
        self.n_nodes = n_nodes
        self.n_next_nodes = n_next_nodes
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_TRIMMER_FILTERS, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)

        if self.n_nodes > 1:
            self._middleware.declare_queue(Q_COORD_SCORE)
            self._middleware.declare_exchange(E_COORD_SCORE, type="fanout")
            for i in range(1, self.n_nodes + 1): # arranco en el id 1 y sigo hasta el numero de nodos
                if i != self.id:
                    self._middleware.bind_queue(Q_COORD_SCORE, E_COORD_SCORE, f"coordination_{i}")
            self.fins_counter = 1

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def process_fin(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            self.fins_counter += 1
            if self.id == 1 and self.fins_counter == self.n_nodes:
                # Reenvía el mensaje FIN y cierra la conexión
                self.logger.custom(f"Soy el nodo lider {self.id}, mando los FINs")
                for node, n_nodes in self.n_next_nodes:
                    for _ in range(n_nodes):
                        if node == 'JOINER_Q3':
                            self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_POSITIVE)
                        if node == 'ENGLISH':
                            self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE_TEXT)
                        if node == 'JOINER_Q5':
                            self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE)
                    self.logger.custom(f"Le mande {n_nodes} FINs a {node}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.shutting_down = True
                self._middleware.connection.close()
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)

            if msg.type == MsgType.REVIEWS:
                negative_textreviews = []
                positive_reviews = []
                negative_reviews = []

                for review in msg.reviews:
                    if review.score == Score.POSITIVE:
                        basic_review = BasicReview(review.app_id)
                        positive_reviews.append(basic_review)
                    else:
                        text_review = TextReview(review.app_id, review.text)
                        basic_review = BasicReview(review.app_id)
                        negative_textreviews.append(text_review)
                        negative_reviews.append(basic_review)

                if positive_reviews:
                    reviews_msg = BasicReviews(msg.id, positive_reviews)
                    self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_POSITIVE)

                if negative_textreviews:
                    reviews_msg = TextReviews(msg.id, negative_textreviews)
                    self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE_TEXT)

                if negative_reviews:
                    reviews_msg = BasicReviews(msg.id, negative_reviews)
                    self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE)

            elif msg.type == MsgType.FIN:
                # Reenvía el mensaje FIN a otros nodos y cierra la conexión
                self._middleware.channel.stop_consuming()
                if self.n_nodes > 1:
                    self._middleware.send_to_queue(Q_COORD_SCORE, msg.encode(), key=f"coordination_{self.id}")
                else:
                    self.logger.custom(f"Soy el nodo lider {self.id}, mando los FINs")
                    for node, n_nodes in self.n_next_nodes:
                        for _ in range(n_nodes):
                            if node == 'JOINER_Q3':
                                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_POSITIVE)
                            if node == 'ENGLISH':
                                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE_TEXT)
                            if node == 'JOINER_Q5':
                                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE)
                        self.logger.custom(f"Le mande {n_nodes} FINs a {node}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER, process_message, auto_ack=False)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(Q_COORD_SCORE, self.process_fin, auto_ack=False)
            else:
                self.shutting_down = True
                self._middleware.connection.close()

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
