from collections import defaultdict
from messages.messages import MsgType, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging

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


    def run(self):

        self.logger.custom("action: listen_to_queue")

        # Procesar mensajes de jeugos
        while True:
            raw_message = self._middleware.receive_from_queue(Q_GENRE_Q5_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.GAME:
                self.games[msg.app_id] = msg
            elif msg.type == MsgType.FIN:
                break

        # Procesar mensajes de reseñas
        while True:
            raw_message = self._middleware.receive_from_queue(Q_SCORE_Q5_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.REVIEW and msg.app_id in self.games:
                self.negative_review_counts[msg.app_id] += 1
            elif msg.type == MsgType.FIN:
                break

        # Calcular el percentil 90 de las reseñas negativas
        negative_counts = list(self.negative_review_counts.values())
        if negative_counts:
            negative_counts.sort()
            index_90 = int(len(negative_counts) * 0.9) - 1
            percentile_90_value = negative_counts[max(index_90, 0)]
        else:
            percentile_90_value = 0

        # Seleccionar juegos en el percentil 90 o superior
        top_games = [
            (self.games[app_id].name, count)
            for app_id, count in self.negative_review_counts.items()
            if count >= percentile_90_value
        ]

        # Crear un mensaje concatenado con los juegos en el percentil 90 o superior
        result_text = f"Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
        for name, count in sorted(top_games, key=lambda x: x[1], reverse=True):
            result_text += f"- {name}: {count} negative reviews\n"

        # Loggear el mensaje completo
        self.logger.custom(result_text)

        # Crear y enviar el mensaje Result con el resultado concatenado
        result_message = Result(id=1, query_number=QueryNumber.Q5.value, result=result_text)
        self._middleware.send_to_queue(Q_QUERY_RESULT_5, result_message.encode())

        # Cierre de la conexión
        self.logger.custom("action: shutting_down | result: in_progress")
        self._middleware.connection.close()
        self.logger.custom("action: shutting_down | result: success")