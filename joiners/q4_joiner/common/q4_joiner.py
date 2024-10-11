from collections import defaultdict
import signal
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q4Result
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

        # Estructuras de almacenamiento
        self.negative_review_counts = defaultdict(int)  # Contará reseñas negativas en inglés
        self.games = {}  # Detalles de juegos de acción/shooter

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.channel.stop_consuming()
        self._middleware.connection.close()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q4_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game
        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_ENGLISH_Q4_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews:
                if review.app_id in self.games:
                    self.negative_review_counts[review.app_id] += 1
        elif msg.type == MsgType.FIN:
            # Filtrar juegos de acción con más de 5,000 reseñas negativas en inglés
            negative_reviews = [
                (self.games[app_id].name, count)
                for app_id, count in self.negative_review_counts.items()
                if count > REVIEWS_NUMBER
            ]

            # Crear y enviar el mensaje Q4Result
            result_message = Q4Result(id=msg.id, negative_reviews=negative_reviews)
            self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())
            
            # Marcar el cierre en proceso y cerrar la conexión
            self.shutting_down = True
            self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks
            self._middleware.receive_from_queue(Q_GENRE_Q4_JOINER, self.process_game_message)
            self._middleware.receive_from_queue(Q_ENGLISH_Q4_JOINER, self.process_review_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
