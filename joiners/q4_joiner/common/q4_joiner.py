from collections import defaultdict
import signal
from messages.messages import MsgType, Q4Result, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging

Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_GENRE_Q4_JOINER = 'genre-joiner_q4'
Q_QUERY_RESULT_4 = "query_result_4"
E_FROM_GENRE = 'from_genre'
K_SHOOTER_GAMES = 'shooter'

REVIEWS_NUMBER = 5000

class Q4Joiner:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)

        # Estructura para almacenar el conteo de reseñas negativas en inglés
        self.negative_review_counts = defaultdict(int)
        self.games = {}  # Almacena detalles de juegos de acción/shooter

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            # Escuchar juegos de género "shooter" y registrar detalles
            while True:
                raw_message = self._middleware.receive_from_queue(Q_GENRE_Q4_JOINER)
                msg = decode_msg(raw_message)
                
                if msg.type == MsgType.GAMES:
                    for game in msg.games:
                        self.games[game.app_id] = game
                elif msg.type == MsgType.FIN:
                    break

            # Escuchar reseñas en inglés y contar las negativas para los juegos de acción
            while True:
                raw_message = self._middleware.receive_from_queue(Q_ENGLISH_Q4_JOINER)
                msg = decode_msg(raw_message)

                if msg.type == MsgType.REVIEWS:
                    for review in msg.reviews:  # Itera sobre cada `Review` en el mensaje `Reviews`
                        if review.app_id in self.games:
                            self.negative_review_counts[review.app_id] += 1

                elif msg.type == MsgType.FIN:

                    # Filtrar los juegos de acción con más de 5,000 reseñas negativas en inglés
                    negative_reviews = [
                        (self.games[app_id].name, count)
                        for app_id, count in self.negative_review_counts.items()
                        if count > REVIEWS_NUMBER
                    ]

                    # Crear el mensaje Q4Result
                    result_message = Q4Result(id=msg.id, negative_reviews=negative_reviews)

                    # Enviar el mensaje codificado a la cola de resultados
                    self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())
                
                    self._middleware.connection.close()
                    # self.logger.custom(f"action: shutting_down | result: success | msg={result_message}")
                    return

        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")