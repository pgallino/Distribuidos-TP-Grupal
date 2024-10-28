from collections import defaultdict
import signal
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q3Result
from node import Node
import heapq

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
        self.games_per_client = defaultdict(lambda: {})  # Almacenará juegos por `app_id`, para cada cliente
        self.review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Contará reseñas positivas por `app_id`, para cada cliente
        self.fins_per_client = defaultdict(lambda: [False, False]) #primer valor corresponde al fin de juegos, y el segundo al de reviews

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            client_games = self.games_per_client[msg.id]
            for game in msg.games:
                client_games[game.app_id] = game

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[0] = True
            if client_fins[0] and client_fins[1]:
                self.join_results(msg.id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            client_reviews = self.review_counts_per_client[msg.id]
            client_games = self.games_per_client[msg.id]
            games_fin_received = self.fins_per_client[msg.id][0]
            for review in msg.reviews:
                if (not games_fin_received) or review.app_id in client_games:
                    client_reviews[review.app_id] += 1

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[1] = True
            if client_fins[0] and client_fins[1]:
                self.join_results(msg.id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q3_JOINER, self.process_game_message), (Q_SCORE_Q3_JOINER, self.process_review_message)], auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
        finally:
            self._shutdown()
    
    def join_results(self, client_id: int):
        # Seleccionar los 5 juegos indie con más reseñas positivas
        client_games = self.games_per_client[client_id]
        client_reviews = self.review_counts_per_client[client_id]
        
        top_5_heap = []
        for app_id, count in client_reviews.items():
            if app_id in client_games:
                if len(top_5_heap) < 5:
                    heapq.heappush(top_5_heap, (count, client_games[app_id].name))
                else:
                    # Si ya hay 5 elementos, reemplazamos el menor si encontramos uno mejor
                    heapq.heappushpop(top_5_heap, (count, client_games[app_id].name))

        top_5_sorted = [(name, num_reviews) for num_reviews, name in sorted(top_5_heap, reverse=True)]

        # Crear y enviar el mensaje Q3Result
        result_message = Q3Result(id=client_id, top_indie_games=top_5_sorted)
        self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())