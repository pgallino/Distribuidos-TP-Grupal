from collections import defaultdict
import signal
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q5Result
from node.node import Node
import numpy as np
from utils.constants import E_FROM_GENRE, E_FROM_SCORE, K_NEGATIVE, K_SHOOTER_GAMES, Q_GENRE_Q5_JOINER, Q_QUERY_RESULT_5, Q_SCORE_Q5_JOINER

class Q5Joiner(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        super().__init__(id, n_nodes, n_next_nodes)

        # Configurar colas y enlaces
        self._middleware.declare_queue(Q_GENRE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q5_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)

        self._middleware.declare_queue(Q_SCORE_Q5_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q5_JOINER, E_FROM_SCORE, K_NEGATIVE)

        self._middleware.declare_queue(Q_QUERY_RESULT_5)

        # Estructuras de almacenamiento
        self.games = {}  # Almacena juegos por `app_id`
        self.negative_review_counts = defaultdict(int)  # Contador de reseñas negativas por `app_id`

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q5_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game
        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q5_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews:
                if review.app_id in self.games:
                    self.negative_review_counts[review.app_id] += 1

        elif msg.type == MsgType.FIN:

            self._middleware.channel.stop_consuming()

            # Calcular el percentil 90 de las reseñas negativas
            counts = np.array(list(self.negative_review_counts.values()))
            threshold = np.percentile(counts, 90)

            # Seleccionar juegos que superan el umbral del percentil 90
            top_games = [
                (app_id, self.games[app_id].name, count)
                for app_id, count in self.negative_review_counts.items()
                if count >= threshold
            ]

            # Ordenar por `app_id` y tomar los primeros 10 resultados
            top_games_sorted = sorted(top_games, key=lambda x: x[0])[:10]

            # Crear y enviar el mensaje Q5Result
            result_message = Q5Result(id=msg.id, top_negative_reviews=top_games_sorted)
            self._middleware.send_to_queue(Q_QUERY_RESULT_5, result_message.encode())
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks
            self._middleware.receive_from_queue(Q_GENRE_Q5_JOINER, self.process_game_message, auto_ack=False)
            self._middleware.receive_from_queue(Q_SCORE_Q5_JOINER, self.process_review_message, auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            self._shutdown()