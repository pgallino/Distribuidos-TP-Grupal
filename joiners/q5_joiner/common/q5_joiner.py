from collections import defaultdict
import signal
from messages.messages import MsgType, Q5Result, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging
import numpy as np

Q_GENRE_Q5_JOINER = "genre-q5-joiner"
Q_SCORE_Q5_JOINER = "score-q5-joiner"
Q_QUERY_RESULT_5 = "query_result_5"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_SHOOTER_GAMES = "shooter"
K_NEGATIVE = 'negative'

class Q5Joiner:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q5_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)
        self._middleware.declare_queue(Q_SCORE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q5_JOINER, E_FROM_SCORE, K_NEGATIVE)

        self._middleware.declare_queue(Q_QUERY_RESULT_5)

       # Estructuras para almacenar datos
        self.games = {}  # Almacenará juegos por `app_id`
        self.negative_review_counts = defaultdict(int)  # Contador de reseñas negativas por `app_id`

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            # Procesar mensajes de jeugos
            while True:
                raw_message = self._middleware.receive_from_queue(Q_GENRE_Q5_JOINER)
                msg = decode_msg(raw_message)

                if msg.type == MsgType.GAMES:
                    for game in msg.games:  # Itera sobre cada `Game` en el mensaje `Games`
                        self.games[game.app_id] = game
                elif msg.type == MsgType.FIN:
                    break

            # Procesar mensajes de reseñas
            while True:
                raw_message = self._middleware.receive_from_queue(Q_SCORE_Q5_JOINER)
                msg = decode_msg(raw_message)

                if msg.type == MsgType.REVIEWS:
                    for review in msg.reviews:  # Itera sobre cada `Review` en el mensaje `Reviews`
                        if review.app_id in self.games:
                            self.negative_review_counts[review.app_id] += 1

                elif msg.type == MsgType.FIN:

                    counts = np.array(list(self.negative_review_counts.values()))
                    threshold = np.percentile(counts, 90)  # Calcula el percentil 90

                    # Seleccionar juegos que superan el umbral del percentil 90
                    top_games = [
                        (app_id, self.games[app_id].name, count)
                        for app_id, count in self.negative_review_counts.items()
                        if count >= threshold
                    ]

                    # Ordenar por `app_id` y tomar los primeros 10 resultados
                    top_games_sorted = sorted(top_games, key=lambda x: x[0])[:10]

                    # Crear el mensaje Q5Result con los juegos ordenados y filtrados
                    result_message = Q5Result(id=msg.id, top_negative_reviews=top_games_sorted)
                    
                    # Enviar el mensaje codificado a la cola de resultados
                    self._middleware.send_to_queue(Q_QUERY_RESULT_5, result_message.encode())

                    # Cierre de la conexión
                    self._middleware.connection.close()
                    return
        
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")