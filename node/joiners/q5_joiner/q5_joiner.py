from collections import defaultdict
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q5Result
from node import Node
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
        self.games_per_client = defaultdict(lambda: {})  # Almacena juegos por `app_id`, para cada cliente
        self.negative_review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Contador de reseñas negativas por `app_id`
        self.fins_per_client = defaultdict(lambda: [False, False]) #primer valor corresponde al fin de juegos, y el segundo al de reviews

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q5_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            client_games = self.games_per_client[msg.id]
            for game in msg.games:
                client_games[game.app_id] = game

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[0] = True
            if client_fins[0] and client_fins[1]:
                print(f"Me llegaron ambos fins de las colas para el cliente {msg.id}, uno los resultados (por games)")
                self.join_results(msg.id)
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q5_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            client_reviews = self.negative_review_counts_per_client[msg.id]
            client_games = self.games_per_client[msg.id]
            games_fin_received = self.fins_per_client[msg.id][0]
            for review in msg.reviews:
                if (not games_fin_received) or review.app_id in client_games:
                    client_reviews[review.app_id] += 1

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[1] = True
            if client_fins[0] and client_fins[1]:
                print(f"Me llegaron ambos fins de las colas para el cliente {msg.id}, uno los resultados (por reviews)")
                self.join_results(msg.id)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        try:
            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q5_JOINER, self.process_game_message), (Q_SCORE_Q5_JOINER, self.process_review_message)], auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
        finally:
            self._shutdown()

    def join_results(self, client_id):
        print(f"Estoy uniendo los resultados del cliente {client_id}")
        client_games = self.games_per_client[client_id]
        client_reviews = self.negative_review_counts_per_client[client_id]

        # Calcular el percentil 90 de las reseñas negativas
        counts = np.array(list(client_reviews.values()))
        threshold = np.percentile(counts, 90)


        # Seleccionar juegos que superan el umbral del percentil 90
        top_games = [
            (app_id, client_games[app_id].name, count)
            for app_id, count in client_reviews.items()
            if app_id in client_games and count >= threshold
        ]

        # Ordenar por `app_id` y tomar los primeros 10 resultados
        top_games_sorted = sorted(top_games, key=lambda x: x[0])[:10]

        # Crear y enviar el mensaje Q5Result
        result_message = Q5Result(id=client_id, top_negative_reviews=top_games_sorted)
        self._middleware.send_to_queue(Q_QUERY_RESULT_5, result_message.encode())
        print(f"Termine de unir los resultados del cliente {client_id}")