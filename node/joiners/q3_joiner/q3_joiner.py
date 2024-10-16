from collections import defaultdict
import signal
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q3Result
from node import Node

from utils.constants import E_FROM_GENRE, E_FROM_SCORE, K_INDIE_BASICGAMES, K_POSITIVE, Q_GENRE_Q3_JOINER, Q_QUERY_RESULT_3, Q_SCORE_Q3_JOINER

class Q3Joiner(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        super().__init__(id, n_nodes, n_next_nodes)

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

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game

        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews:
                if review.app_id in self.games:
                    self.review_counts[review.app_id] += 1

        elif msg.type == MsgType.FIN:

            self._middleware.channel.stop_consuming()

            # Seleccionar los 5 juegos indie con más reseñas positivas
            top_indie_games = sorted(
                [(self.games[app_id].name, count) for app_id, count in self.review_counts.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]

            # Crear y enviar el mensaje Q3Result
            result_message = Q3Result(id=msg.id, top_indie_games=top_indie_games)
            self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks
            self._middleware.receive_from_queue(Q_GENRE_Q3_JOINER, self.process_game_message, auto_ack=False)
            self._middleware.receive_from_queue(Q_SCORE_Q3_JOINER, self.process_review_message, auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self._shutdown()