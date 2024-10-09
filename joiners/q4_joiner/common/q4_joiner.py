from collections import defaultdict
from messages.messages import MsgType, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging

Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_GENRE_Q4_JOINER = 'genre-joiner_q4'
Q_QUERY_RESULT_4 = "query_result_4"
E_FROM_GENRE = 'from_genre'
K_SHOOTER_GAMES = 'shooter'

REVIEWS_NUMBER = 3

class Q4Joiner:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)

        # Estructura para almacenar el conteo de reseñas negativas en inglés
        self.negative_review_counts = defaultdict(int)
        self.games = {}  # Almacena detalles de juegos de acción/shooter


    def run(self):
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
                # Mensaje con juegos que tienen más de 5,000 reseñas negativas en inglés
                result_text = "Q4: Action games with more than 5,000 negative reviews in English:\n"
                for app_id, count in self.negative_review_counts.items():
                    if count > REVIEWS_NUMBER:
                        game_name = self.games[app_id].name
                        result_text += f"- {game_name}: {count} negative reviews\n"

                # Crear y enviar el mensaje Result con el resultado concatenado
                result_message = Result(id=msg.id, query_number=QueryNumber.Q4.value, result=result_text)
                self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())
            
                # self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.connection.close()
                # self.logger.custom("action: shutting_down | result: success")
                return
