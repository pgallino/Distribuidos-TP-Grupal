import signal
from messages.messages import MsgType, Score, decode_msg, BATCH_SIZE, Reviews, Review
from middleware.middleware import Middleware
import logging

Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_REVIEW = 'review'
Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_POSITIVE = 'positive'
K_NEGATIVE = 'negative'

class ScoreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_TRIMMER_FILTERS, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)
        # Listas para acumular reseñas
        self.positive_reviews = []
        self.negative_reviews = []

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
                raw_message = self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER)
                msg = decode_msg(raw_message)
                # self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")
                if msg.type == MsgType.REVIEWS:
                    for review in msg.reviews:  # Procesa cada `Review` en el mensaje `REVIEWS`
                        if review.score == Score.POSITIVE:
                            self.positive_reviews.append(review)
                            if len(self.positive_reviews) >= BATCH_SIZE:
                                self._send_reviews_batch(msg.id, self.positive_reviews, K_POSITIVE)
                        else:
                            self.negative_reviews.append(review)
                            if len(self.negative_reviews) >= BATCH_SIZE:
                                self._send_reviews_batch(msg.id, self.negative_reviews, K_NEGATIVE)

                elif msg.type == MsgType.FIN:
                    # Enviar cualquier batch restante de reseñas
                    if self.positive_reviews:
                        self._send_reviews_batch(msg.id, self.positive_reviews, K_POSITIVE)
                    if self.negative_reviews:
                        self._send_reviews_batch(msg.id, self.negative_reviews, K_NEGATIVE)
                    # Se reenvia el FIN al resto de nodos
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_POSITIVE)
                    self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_NEGATIVE)
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return
                
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

    def _send_reviews_batch(self, id, reviews_batch, key):
        """
        Envía un batch de reseñas como un solo mensaje `Reviews`.
        """
        reviews_msg = Reviews(id, reviews=reviews_batch)  # Crear el mensaje `Reviews`
        self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), key=key)
        reviews_batch.clear()  # Limpiar el batch después de enviar