from collections import defaultdict
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_GENRE_Q4_JOINER = 'genre-joiner_q4'
E_FROM_GENRE = 'from_genre'
K_SHOOTER_GAMES = 'shooter'

class Q4Joiner:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)

        # Estructura para almacenar el conteo de reseñas negativas en inglés
        self.negative_review_counts = defaultdict(int)
        self.games = {}  # Almacena detalles de juegos de acción/shooter


    def run(self):
        self.logger.custom("action: listen_to_queue")

        # Escuchar juegos de género "shooter" y registrar detalles
        while True:
            raw_message = self._middleware.receive_from_queue(Q_GENRE_Q4_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.GAME:
                self.games[msg.app_id] = msg  # Almacena solo juegos shooter que representan "action"
            elif msg.type == MsgType.FIN:
                break

        # Escuchar reseñas en inglés y contar las negativas para los juegos de acción
        while True:
            raw_message = self._middleware.receive_from_queue(Q_ENGLISH_Q4_JOINER)
            msg = decode_msg(raw_message[2:])
            if msg.type == MsgType.REVIEW and msg.app_id in self.games:
                # Incrementar el conteo de reseñas negativas en inglés
                self.negative_review_counts[msg.app_id] += 1
            elif msg.type == MsgType.FIN:
                self.logger.custom("action: shutting_down | result: in_progress")
                self._print_results()
                self._middleware.connection.close()
                self.logger.custom("action: shutting_down | result: success")
                break

    def _print_results(self):
        # Mensaje con juegos que tienen más de 5,000 reseñas negativas en inglés
        top_message = "Q4: Action games with more than 5,000 negative reviews in English:\n"
        for app_id, count in self.negative_review_counts.items():
            if count > 5000:
                game_name = self.games[app_id].name
                top_message += f"- {game_name}: {count} negative reviews\n"

        # Mensaje con el conteo total de reseñas negativas en inglés por juego
        total_message = "\nTotal negative reviews in English per game:\n"
        for app_id, count in self.negative_review_counts.items():
            game_name = self.games[app_id].name
            total_message += f"- {game_name}: {count} negative reviews\n"

        # Loggear ambos mensajes
        self.logger.custom(top_message)
        self.logger.custom(total_message)
