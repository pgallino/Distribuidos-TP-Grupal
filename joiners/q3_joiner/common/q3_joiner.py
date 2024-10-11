from collections import defaultdict
import signal
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q3Result
from middleware.middleware import Middleware
import logging

from utils.constants import E_FROM_GENRE, E_FROM_SCORE, K_INDIE_BASICGAMES, K_POSITIVE, Q_GENRE_Q3_JOINER, Q_QUERY_RESULT_3, Q_SCORE_Q3_JOINER

class Q3Joiner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        self._middleware = Middleware()

        # Declarar colas y binders
        self._middleware.declare_queue(Q_GENRE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q3_JOINER, E_FROM_GENRE, K_INDIE_BASICGAMES)

        self._middleware.declare_queue(Q_SCORE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q3_JOINER, E_FROM_SCORE, K_POSITIVE)

        self._middleware.declare_queue(Q_QUERY_RESULT_3)

        # Estructuras para almacenar datos
        self.games = {}  # Almacenará juegos por `app_id`
        self.review_counts = defaultdict(int)  # Contará reseñas positivas por `app_id`

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.channel.stop_consuming()
        self._middleware.connection.close()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q3_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game
        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q3_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews:
                if review.app_id in self.games:
                    self.review_counts[review.app_id] += 1
        elif msg.type == MsgType.FIN:
            # Seleccionar los 5 juegos indie con más reseñas positivas
            top_indie_games = sorted(
                [(self.games[app_id].name, count) for app_id, count in self.review_counts.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]

            # Crear y enviar el mensaje Q3Result
            result_message = Q3Result(id=msg.id, top_indie_games=top_indie_games)
            self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())
            self.shutting_down = True
            self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks
            self._middleware.receive_from_queue(Q_GENRE_Q3_JOINER, self.process_game_message)
            self._middleware.receive_from_queue(Q_SCORE_Q3_JOINER, self.process_review_message)
        
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
