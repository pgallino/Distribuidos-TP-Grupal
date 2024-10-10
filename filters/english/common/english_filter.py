import signal
from messages.messages import BasicReview, BasicReviews, MsgType, decode_msg, Reviews
from middleware.middleware import Middleware
import logging
import langid
# import random

Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_NEGATIVE_TEXT = 'negative_text'
Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'

class EnglishFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_SCORE_ENGLISH)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_ENGLISH, E_FROM_SCORE, K_NEGATIVE_TEXT)
    
    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def is_english(self, text):
        # Detectar idioma usando langid
        lang, _ = langid.classify(text)
        # self.logger.custom(f"is english: {lang}")
        return lang == 'en'  # Retorna True si el idioma detectado es inglés
        # return random.choice([True, False])

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)
            en_reviews = []

            if msg.type == MsgType.REVIEWS:
                # Filtrar reseñas en inglés
                for review in msg.reviews:
                    if self.is_english(review.text):
                        basic_review = BasicReview(review.app_id)
                        en_reviews.append(basic_review)
                
                # Enviar el batch de reseñas en inglés si contiene elementos
                if en_reviews:
                    english_reviews_msg = BasicReviews(id=msg.id, reviews=en_reviews)
                    self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, english_reviews_msg.encode())

            elif msg.type == MsgType.FIN:
                # Reenvía el mensaje FIN y cierra la conexión
                self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())
                self.shutting_down = True
                self._middleware.connection.close()

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_SCORE_ENGLISH, process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

