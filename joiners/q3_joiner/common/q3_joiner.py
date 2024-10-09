from collections import defaultdict
import signal
from messages.messages import MsgType, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging

Q_GENRE_Q3_JOINER = "genre-q3-joiner"
Q_SCORE_Q3_JOINER = "score-q3-joiner"
Q_QUERY_RESULT_3 = "query_result_3"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_INDIE_BASICGAMES = "indiebasic"
K_POSITIVE = 'positive'

class Q3Joiner:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
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
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                raw_message = self._middleware.receive_from_queue(Q_GENRE_Q3_JOINER)
                msg = decode_msg(raw_message)
                if msg.type == MsgType.GAMES:
                    # Registrar el juego indie
                    for game in msg.games:
                        self.games[game.app_id] = game
                        # self.logger.custom(f"Registered game: {game.name} (ID: {game.app_id})")
                elif msg.type == MsgType.FIN:
                    break
            
            while True:
                raw_message = self._middleware.receive_from_queue(Q_SCORE_Q3_JOINER)
                msg = decode_msg(raw_message)
                if msg.type == MsgType.REVIEWS:
                    # Contar reseñas positivas
                    for review in msg.reviews:
                        if review.app_id in self.games:
                            self.review_counts[review.app_id] += 1
                        # # self.logger.custom(f"Incremented positive review count for app_id: {msg.app_id}")
                elif msg.type == MsgType.FIN:
                    # Seleccionar los 5 juegos indie con más reseñas positivas
                    top_indie_games = sorted(
                        [(self.games[app_id].name, count) for app_id, count in self.review_counts.items()],
                        key=lambda x: x[1], 
                        reverse=True
                    )[:5]

                    # Crear el mensaje de resultado para el top 5
                    result_text = "Q3: Top 5 Indie Games with Most Positive Reviews:\n"
                    for rank, (name, count) in enumerate(top_indie_games, start=1):
                        result_text += f"{rank}. {name}: {count} positive reviews\n"

                    # Loggear el mensaje de resultado
                    # self.logger.custom(result_text)

                    # Crear el mensaje Result
                    result_message = Result(id=msg.id, query_number=QueryNumber.Q3.value, result=result_text)

                    # Enviar el mensaje Result al exchange de resultados
                    self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())

                    # Cierre de la conexión
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return

        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")