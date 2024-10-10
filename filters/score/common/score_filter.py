import signal
from messages.messages import BasicReview, BasicReviews, MsgType, Score, TextReview, TextReviews, decode_msg, BATCH_SIZE, Reviews, Review
from middleware.middleware import Middleware
import logging

Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_REVIEW = 'review'
Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_NEGATIVE = 'negative'
K_NEGATIVE_TEXT = 'negative_text'
K_POSITIVE = 'positive'

class ScoreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_TRIMMER_FILTERS, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

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
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_NEGATIVE_TEXT)
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_POSITIVE)
                self._middleware.send_to_queue(E_FROM_SCORE, msg.encode(), key=K_NEGATIVE)
                self.shutting_down = True
                self._middleware.connection.close()

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER, process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
