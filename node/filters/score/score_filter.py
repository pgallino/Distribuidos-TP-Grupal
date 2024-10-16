from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.reviews_msg import BasicReview, BasicReviews, Score, TextReview, TextReviews
from node import Node  # Importa la clase base Node
from utils.constants import E_COORD_SCORE, E_FROM_SCORE, E_TRIMMER_FILTERS, K_NEGATIVE, K_NEGATIVE_TEXT, K_POSITIVE, K_REVIEW, Q_COORD_SCORE, Q_TRIMMER_SCORE_FILTER


class ScoreFilter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes)
        
        # Configura las colas y los intercambios específicos para ScoreFilter
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_TRIMMER_FILTERS, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)

        # Configura la cola de coordinación
        self._setup_coordination_queue(Q_COORD_SCORE, E_COORD_SCORE)

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:
            self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER, self._process_message, auto_ack=False)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        """Procesa mensajes de tipo FIN y coordina con otros nodos."""
        keys = [(K_POSITIVE if node == 'JOINER_Q3' 
                 else K_NEGATIVE_TEXT if node == 'JOINER_Q4' 
                 else K_NEGATIVE, n_nodes) 
                 for node, n_nodes in self.n_next_nodes]
        self._process_fin(raw_message, keys, E_FROM_SCORE)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola."""
        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.REVIEWS:
            self._process_reviews_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_reviews_message(self, msg):
        """Procesa mensajes de tipo REVIEWS y distribuye según el score."""
        negative_textreviews, positive_reviews, negative_reviews = [], [], []

        for review in msg.reviews:
            if review.score == Score.POSITIVE:
                positive_reviews.append(BasicReview(review.app_id))
            else:
                negative_textreviews.append(TextReview(review.app_id, review.text))
                negative_reviews.append(BasicReview(review.app_id))

        if positive_reviews:
            reviews_msg = BasicReviews(msg.id, positive_reviews)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_POSITIVE)

        if negative_textreviews:
            reviews_msg = TextReviews(msg.id, negative_textreviews)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE_TEXT)

        if negative_reviews:
            reviews_msg = BasicReviews(msg.id, negative_reviews)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE)

    def _process_fin_message(self, msg):
        """Reenvía el mensaje FIN y cierra la conexión si es necesario."""
        self._middleware.channel.stop_consuming()
        
        if self.n_nodes > 1:
            key=f"coordination_{self.id}"
            self._middleware.send_to_queue(E_COORD_SCORE, msg.encode(), key=key)
        else:
            for node, n_nodes in self.n_next_nodes:
                for _ in range(n_nodes):
                    if node == 'JOINER_Q3':
                        self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_POSITIVE)
                    elif node == 'JOINER_Q4':
                        self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE_TEXT)
                    elif node == 'JOINER_Q5':
                        self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), K_NEGATIVE)
