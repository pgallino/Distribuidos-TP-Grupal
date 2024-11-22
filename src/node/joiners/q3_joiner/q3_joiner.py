from collections import defaultdict
import logging
from messages.messages import MsgType, PushDataMessage, ResultMessage, decode_msg
from messages.results_msg import Q3Result, QueryNumber
from node import Node
import heapq

from utils.constants import E_FROM_GENRE, E_FROM_SCORE, K_INDIE_BASICGAMES, K_POSITIVE, Q_GENRE_Q3_JOINER, Q_QUERY_RESULT_3, Q_SCORE_Q3_JOINER

class Q3Joiner(Node):
    def __init__(self, id: int, n_nodes: int, container_name: str):
        super().__init__(id=id, n_nodes=n_nodes, container_name=container_name)

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

    def run(self):

        try:

            self.init_ka(self.container_name)
            self._synchronize_with_replica()

            # Verificar clientes con ambos FIN y procesar joins pendientes
            for client_id, fins in list(self.fins_per_client.items()):
                if fins[0] and fins[1]:
                    logging.info(f"Node: Procesando join pendiente para client_id: {client_id}")
                    self.join_results(client_id)

            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q3_JOINER, self.process_game_message), (Q_SCORE_Q3_JOINER, self.process_review_message)], auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            client_games = self.games_per_client[msg.id]
            for game in msg.items:
                client_games[game.app_id] = game.name

            # Enviar solo el estado de juegos actualizado
            self.push_games_state(msg.id)

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[0] = True
            if client_fins[0] and client_fins[1]:
                self.join_results(msg.id)

            # Enviar solo el estado de FIN actualizado
            self.push_fin_state(msg.id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            client_reviews = self.review_counts_per_client[msg.id]
            client_games = self.games_per_client[msg.id]
            games_fin_received = self.fins_per_client[msg.id][0]
            for review in msg.items:
                if (not games_fin_received) or review.app_id in client_games:
                    client_reviews[review.app_id] += 1

            # Enviar solo el estado de reseñas actualizado
            self.push_reviews_state(msg.id)

        elif msg.type == MsgType.FIN:
            client_fins = self.fins_per_client[msg.id]
            client_fins[1] = True

            # Enviar solo el estado de FIN actualizado
            self.push_fin_state(msg.id)

            if client_fins[0] and client_fins[1]:
                self.join_results(msg.id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def join_results(self, client_id: int):

        self.client_id_while_joining = client_id
        # Seleccionar los 5 juegos indie con más reseñas positivas
        client_games = self.games_per_client[client_id]
        client_reviews = self.review_counts_per_client[client_id]
        
        top_5_heap = []
        for app_id, count in client_reviews.items():
            if app_id in client_games:
                game_name = client_games[app_id]
                if len(top_5_heap) < 5:
                    heapq.heappush(top_5_heap, (count, game_name))
                else:
                    # Si ya hay 5 elementos, reemplazamos el menor si encontramos uno mejor
                    heapq.heappushpop(top_5_heap, (count, game_name))

        top_5_sorted = [(name, num_reviews) for num_reviews, name in sorted(top_5_heap, reverse=True)]

        # Crear y enviar el mensaje Q3Result
        q3_result = Q3Result(top_indie_games=top_5_sorted)
        result_message = ResultMessage(id=client_id, result_type=QueryNumber.Q3, result=q3_result)
        self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())

        # Borro los diccionarios de clientes ya resueltos
        del self.games_per_client[client_id]
        del self.review_counts_per_client[client_id]
        del self.fins_per_client[client_id]

        self.push_delete_client_state(client_id)

    def push_games_state(self, client_id):
        """Envía el estado de los juegos para un cliente específico a la réplica."""
        state = {
            "client_id": client_id,
            "games": self.games_per_client.get(client_id, {})
        }
        push_msg = PushDataMessage(data=state)
        self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())
        # logging.info(f"action: Push games state | client_id: {client_id} | state: {state}")


    def push_reviews_state(self, client_id):
        """Envía el estado de las reseñas para un cliente específico a la réplica."""
        state = {
            "client_id": client_id,
            "reviews": dict(self.review_counts_per_client.get(client_id, {}))
        }
        push_msg = PushDataMessage(data=state)
        self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())
        # logging.info(f"action: Push reviews state | client_id: {client_id} | state: {state}")

    def push_fin_state(self, client_id):
        """Envía el estado de fin para un cliente específico a la réplica."""
        state = {
            "client_id": client_id,
            "fins": self.fins_per_client.get(client_id, [False, False])
        }
        push_msg = PushDataMessage(data=state)
        self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())
        # logging.info(f"action: Push FIN state | client_id: {client_id} | state: {state}")

    def push_delete_client_state(self, client_id: int):
        """Envía un mensaje a la réplica para borrar el estado de un cliente."""
        try:
            push_msg = PushDataMessage(data={"delete": client_id})
            self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())
            logging.info(f"Node: Enviado mensaje para borrar estado de client_id: {client_id}")
        except Exception as e:
            logging.error(f"Error al enviar delete_state para client_id: {client_id} | Error: {e}")


    def load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar juegos por cliente
        if "games_per_client" in state:
            for client_id, games in state["games_per_client"].items():
                self.games_per_client[client_id] = games
            logging.info(f"Replica: Juegos actualizados desde estado recibido.")

        # Actualizar reseñas por cliente
        if "review_counts_per_client" in state:
            for client_id, reviews in state["review_counts_per_client"].items():
                if client_id not in self.review_counts_per_client:
                    self.review_counts_per_client[client_id] = defaultdict(int)
                self.review_counts_per_client[client_id].update(reviews)
            logging.info(f"Replica: Reseñas actualizadas desde estado recibido.")

        # Actualizar fins por cliente
        if "fins_per_client" in state:
            for client_id, fins in state["fins_per_client"].items():
                self.fins_per_client[client_id] = fins
            logging.info(f"Replica: Estados FIN actualizados desde estado recibido.")

        logging.info(f"Replica: Estado completo cargado. Campos cargados: {list(state.keys())}")





