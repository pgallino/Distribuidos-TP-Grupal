from collections import defaultdict
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q4Result
from node.node import Node

from utils.constants import E_FROM_SCORE, E_FROM_GENRE, K_SHOOTER_GAMES, Q_ENGLISH_Q4_JOINER, Q_GENRE_Q4_JOINER, Q_QUERY_RESULT_4

class Q4Joiner(Node):

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], batch_size: int, n_reviews: int):
        super().__init__(id, n_nodes, n_next_nodes)

        self.n_reviews = n_reviews
        
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)

        # Estructuras de almacenamiento
        self.negative_review_counts = defaultdict(int)  # Contará reseñas negativas en inglés
        self.games = {}  # Detalles de juegos de acción/shooter

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q4_JOINER`."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.GAMES:
            for game in msg.games:
                self.games[game.app_id] = game
        elif msg.type == MsgType.FIN:
            self._middleware.channel.stop_consuming()
                            
    def process_negative_review_message(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        if msg.type == MsgType.REVIEWS:
            for review in msg.reviews:
                if review.app_id in self.games:
                    self.negative_review_counts[review.app_id] += 1
        elif msg.type == MsgType.FIN:
            # Filtrar juegos de acción con más de 5,000 reseñas negativas en inglés
            negative_reviews = sorted(
                [
                    (app_id, self.games[app_id].name, count)
                    for app_id, count in self.negative_review_counts.items()
                    if count > self.n_reviews
                ],
                key=lambda x: x[0],  # Ordenar por app_id
                reverse=True  # Orden descendente
            )[:25]  # Tomar los 25 primeros


            # Crear y enviar el mensaje Q4Result
            result_message = Q4Result(id=msg.id, negative_reviews=negative_reviews)
            self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())
            
            # Marcar el cierre en proceso y cerrar la conexión
            self._shutdown()

    def run(self):

        try:
            self._middleware.receive_from_queue(Q_GENRE_Q4_JOINER, self.process_game_message)
            self._middleware.receive_from_queue(Q_ENGLISH_Q4_JOINER, self.process_negative_review_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")