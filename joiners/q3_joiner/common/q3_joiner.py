from collections import defaultdict
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_GENRE_Q3_JOINER = "genre-q3-joiner"
Q_SCORE_Q3_JOINER = "score-q3-joiner"
E_FROM_GENRE = "from_genre"
E_FROM_SCORE = "from_score"
K_INDIE_GAMES = "indie"
K_POSITIVE = 'positive'

class Q3Joiner:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q3_JOINER, E_FROM_GENRE, K_INDIE_GAMES)

        self._middleware.declare_queue(Q_SCORE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q3_JOINER, E_FROM_SCORE, K_POSITIVE)

        # Estructuras para almacenar datos
        self.games = {}  # Almacenará juegos por `app_id`
        self.review_counts = defaultdict(int)  # Contará reseñas positivas por `app_id`

    def run(self):

        self.logger.custom("action: listen_to_queue")
        while True:
            raw_message = self._middleware.receive_from_queue(Q_GENRE_Q3_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.GAME:
                # Registrar el juego indie
                self.games[msg.app_id] = msg
                # self.logger.custom(f"Registered game: {msg.name} (ID: {msg.app_id})")
            elif msg.type == MsgType.FIN:
                break
        
        while True:
            raw_message = self._middleware.receive_from_queue(Q_SCORE_Q3_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.REVIEW:
                # Contar reseñas positivas
                if msg.app_id in self.games:
                    self.review_counts[msg.app_id] += 1
                    # self.logger.custom(f"Incremented positive review count for app_id: {msg.app_id}")
            elif msg.type == MsgType.FIN:
                break

        # Seleccionar los 5 juegos indie con más reseñas positivas
        top_indie_games = sorted(
            [(self.games[app_id].name, count) for app_id, count in self.review_counts.items()],
            key=lambda x: x[1], 
            reverse=True
        )[:5]

        # Crear un mensaje concatenado con el top 5
        top_message = "Top 5 Indie Games with Most Positive Reviews:\n"
        for rank, (name, count) in enumerate(top_indie_games, start=1):
            top_message += f"{rank}. {name}: {count} positive reviews\n"

        # Loggear el mensaje grande con el top 5
        self.logger.custom(top_message)

        # Cierre de la conexión
        self.logger.custom("action: shutting_down | result: in_progress")
        self._middleware.connection.close()
        self.logger.custom("action: shutting_down | result: success")