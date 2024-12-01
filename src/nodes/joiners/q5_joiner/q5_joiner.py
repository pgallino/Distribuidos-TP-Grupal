from collections import defaultdict
import logging
from typing import List, Tuple
from messages.messages import MsgType, ResultMessage, decode_msg, PushDataMessage
from messages.results_msg import Q5Result, QueryNumber
from node import Node
import numpy as np # type: ignore # genera 7 pids en docker stats
from utils.constants import E_FROM_GENRE, E_FROM_SCORE, K_NEGATIVE, K_SHOOTER_GAMES, Q_GENRE_Q5_JOINER, Q_QUERY_RESULT_5, Q_SCORE_Q5_JOINER

class Q5Joiner(Node):
    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name)

        self.n_replicas = n_replicas

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

    def run(self):

        try:

            if self.n_replicas > 0: # verifico si se instanciaron replicas
                self._synchronize_with_replicas()

            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q5_JOINER, self.process_game_message), (Q_SCORE_Q5_JOINER, self.process_review_message)], auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q5_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            
            # Inicializar diccionario de actualizaciones
            update = {}
            client_games = self.games_per_client[msg.client_id]
            for game in msg.items:
                client_games[game.app_id] = game.name
                # Registrar el cambio en el diccionario de actualizaciones
                update[game.app_id] = game.name

            self.push_update('games', msg.client_id, update)

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[0] = True

            self.push_update('fins', msg.client_id, client_fins)

            if client_fins[0] and client_fins[1]:
                self.join_results(msg.client_id)
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q5_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:

            # Inicializar diccionario de actualizaciones
            update = {}
            client_reviews = self.negative_review_counts_per_client[msg.client_id]
            client_games = self.games_per_client[msg.client_id]
            games_fin_received = self.fins_per_client[msg.client_id][0]
            for review in msg.items:
                if (not games_fin_received) or review.app_id in client_games:
                    client_reviews[review.app_id] += 1
                    update[review.app_id] = client_reviews[review.app_id]
            
            self.push_update('reviews', msg.client_id, update)

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[1] = True

            self.push_update('fins', msg.client_id, client_fins)

            if client_fins[0] and client_fins[1]:
                self.join_results(msg.client_id)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def join_results(self, client_id):
        client_games = self.games_per_client[client_id]
        client_reviews = self.negative_review_counts_per_client[client_id]
        logging.info(f"LA LONGITUD DE REVIEWS ES: {len(client_reviews)}")
        client_reviews = {app_id: count for app_id, count in client_reviews.items() if app_id in client_games}

        # Calcular el percentil 90 de las reseñas negativas
        counts = np.array(list(client_reviews.values()))
        threshold = np.percentile(counts, 90)

        # Seleccionar juegos que superan el umbral del percentil 90
        top_games = [
            (app_id, client_games[app_id], count)
            for app_id, count in client_reviews.items()
            if count >= threshold
        ]

        # Ordenar por `app_id` y tomar los primeros 10 resultados
        top_games_sorted = sorted(top_games, key=lambda x: x[0])[:10]

        # Crear y enviar el mensaje Q5Result
        q5_result = Q5Result(top_negative_reviews=top_games_sorted)
        result_message = ResultMessage( client_id=client_id, result_type=QueryNumber.Q5, result=q5_result)
        self._middleware.send_to_queue(Q_QUERY_RESULT_5, result_message.encode())

        # Borro los diccionarios de clientes ya resueltos
        del self.games_per_client[client_id]
        del self.negative_review_counts_per_client[client_id]

        self.push_update('delete', client_id)
        #TODO: SI SE CAE DESPUES DEL DELETE Y NO HABER HECHO EL ACK PODES PERDER EL CLIENTE PARA SIEMPRE
        # PERO SI LLEGASTE HASTA ACA ES PORQUE YA ENVIASTE LA RESPUESTA AL CLIENTE POR LO QUE NO TE IMPORTA PERDERLO

    def load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar juegos por cliente
        if "games_per_client" in state:
            for client_id, games in state["games_per_client"].items():
                self.games_per_client[client_id] = games
            logging.info(f"Replica: Juegos actualizados desde estado recibido.")

        # Actualizar reseñas por cliente
        if "negative_review_counts_per_client" in state:
            for client_id, reviews in state["negative_review_counts_per_client"].items():
                if client_id not in self.negative_review_counts_per_client:
                    self.negative_review_counts_per_client[client_id] = defaultdict(int)
                self.negative_review_counts_per_client[client_id].update(reviews)
            logging.info(f"Replica: Reseñas actualizadas desde estado recibido.")

        # Actualizar fins por cliente
        if "fins_per_client" in state:
            for client_id, fins in state["fins_per_client"].items():
                self.fins_per_client[client_id] = fins
            logging.info(f"Replica: Estados FIN actualizados desde estado recibido.")

        logging.info(f"Replica: Estado completo cargado. Campos cargados: {list(state.keys())}")