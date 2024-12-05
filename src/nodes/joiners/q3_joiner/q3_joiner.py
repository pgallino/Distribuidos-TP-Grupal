from collections import defaultdict
import logging
from messages.messages import MsgType, PushDataMessage, ResultMessage, decode_msg
from messages.results_msg import Q3Result, QueryNumber
from node import Node
import heapq

from utils.container_constants import ENDPOINTS_PROB_FAILURE
from utils.middleware_constants import E_FROM_GENRE, E_FROM_PROP, E_FROM_SCORE, K_FIN, K_INDIE_BASICGAMES, K_POSITIVE, Q_GENRE_Q3_JOINER, Q_QUERY_RESULT_3, Q_SCORE_Q3_JOINER
from utils.utils import NodeType, log_with_location, simulate_random_failure

class Q3Joiner(Node):
    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name)

        self.n_replicas = n_replicas

        # Declarar colas y binders
        self._middleware.declare_queue(Q_GENRE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_Q3_JOINER, E_FROM_GENRE, K_INDIE_BASICGAMES)

        self._middleware.declare_queue(Q_SCORE_Q3_JOINER)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_Q3_JOINER, E_FROM_SCORE, K_POSITIVE)

        self._middleware.declare_queue(Q_QUERY_RESULT_3)

        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(Q_GENRE_Q3_JOINER, E_FROM_PROP, key=K_FIN+f'.{container_name}_games') # hay que modificarlo en el propagator
        self._middleware.bind_queue(Q_SCORE_Q3_JOINER, E_FROM_PROP, key=K_FIN+f'.{container_name}_reviews')

        # Estructuras para almacenar datos
        self.games_per_client = defaultdict(lambda: {})  # Almacenará juegos por `app_id`, para cada cliente
        self.review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Contará reseñas positivas por `app_id`, para cada cliente
        self.fins_per_client = defaultdict(lambda: [False, False]) #primer valor corresponde al fin de juegos, y el segundo al de reviews
        self.last_msg_id = 0

    def get_type(self) -> NodeType:
        return NodeType.Q3_JOINER

    def run(self):

        try:

            if self.n_replicas > 0: # verifico si se instanciaron replicas

                # ==================================================================
                # CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS
                simulate_random_failure(self, log_with_location("CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
                # ==================================================================
                self._synchronize_with_replicas()

            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q3_JOINER, self.process_game_message), (Q_SCORE_Q3_JOINER, self.process_review_message)], auto_ack=False)
        
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: run | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:

            # Inicializar diccionario de actualizaciones
            update = {}
            client_games = self.games_per_client[msg.client_id]
            for game in msg.items:
                client_games[game.app_id] = game.name
                # Registrar el cambio en el diccionario de actualizaciones
                update[game.app_id] = game.name

            # ==================================================================
            # CAIDA ANTES DE ENVIAR ACTUALIZACION A LAS REPLICAS
            simulate_random_failure(self, log_with_location("CAIDA ANTES DE ENVIAR ACTUALIZACION DE JUEGOS A LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            self.push_update('games', msg.client_id, update)

            # ==================================================================
            # CAIDA DESPUES DE ENVIAR ACTUALIZACION A LAS REPLICAS
            # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ENVIAR ACTUALIZACION DE JUEGOS A LAS REPLICAS ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

        elif msg.type == MsgType.FIN:
            # logging.info(f"Llego FIN GAMES de cliente {msg.client_id}")
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[0] = True

            # ==================================================================
            # CAIDA ANTES DE ENVIAR ACTUALIZACION DE FIN GAMES A LAS REPLICAS
            simulate_random_failure(self, log_with_location("CAIDA ANTES DE ENVIAR FIN GAMES A LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            self.push_update('fins', msg.client_id, client_fins)

            # ==================================================================
            # CAIDA DESPUES DE ENVIAR ACTUALIZACION DE FIN GAMES A LAS REPLICAS
            # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ENVIAR FIN GAMES A LAS REPLICAS ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            if client_fins[0] and client_fins[1]:
                self.join_results(msg.client_id)
        
        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # ==================================================================
        # CAIDA DESPUES DE HACER EL ACK EN GAMES
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE HACER EL ACK EN GAMES"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_SCORE_Q3_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:

            # Inicializar diccionario de actualizaciones
            update = {}
            client_reviews = self.review_counts_per_client[msg.client_id]
            client_games = self.games_per_client[msg.client_id]
            games_fin_received = self.fins_per_client[msg.client_id][0]
            for review in msg.items:
                if (not games_fin_received) or review.app_id in client_games:
                    client_reviews[review.app_id] += 1
                    update[review.app_id] = client_reviews[review.app_id]

            # ==================================================================
            # CAIDA ANTES DE ENVIAR ACTUALIZACION DE REVIEWS A LAS REPLICAS
            simulate_random_failure(self, log_with_location("CAIDA ANTES DE ENVIAR ACTUALIZACION DE REVIEWS A LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            self.push_update('reviews', msg.client_id, update)

            # ==================================================================
            # CAIDA DESPUES DE ENVIAR ACTUALIZACION DE REVIEWS A LAS REPLICAS
            # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ENVIAR ACTUALIZACION DE REVIEWS A LAS REPLICAS ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

        elif msg.type == MsgType.FIN:
            # logging.info(f"Llego FIN REVIEWs de cliente {msg.client_id}")
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[1] = True

            # ==================================================================
            # CAIDA ANTES DE ENVIAR FIN REVIEWS A LAS REPLICAS
            simulate_random_failure(self, log_with_location("CAIDA ANTES DE ENVIAR FIN REVIEWS A LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            self.push_update('fins', msg.client_id, client_fins)

            # ==================================================================
            # CAIDA DESPUES DE ENVIAR FIN REVIEWS A LAS REPLICAS
            # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ENVIAR FIN REVIEWS A LAS REPLICAS ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            if client_fins[0] and client_fins[1]:
                self.join_results(msg.client_id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # ==================================================================
        # CAIDA DESPUES DE HACER EL ACK EN REVIEWS
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE HACER EL ACK EN REVIEWS"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================
    
    def join_results(self, client_id: int):

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
        result_message = ResultMessage(client_id=client_id, result_type=QueryNumber.Q3, result=q3_result)

        # ==================================================================
        # CAIDA ANTES DE ENVIAR RESULTADO Q3
        simulate_random_failure(self, log_with_location("CAIDA ANTES DE ENVIAR RESULTADO Q3"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        self._middleware.send_to_queue(Q_QUERY_RESULT_3, result_message.encode())

        # ==================================================================
        # CAIDA DESPUES DE ENVIAR RESULTADO Q3
        # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ENVIAR RESULTADO Q3 ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        # Borro los diccionarios de clientes ya resueltos
        del self.games_per_client[client_id]
        del self.review_counts_per_client[client_id]
        del self.fins_per_client[client_id]

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

        # Actualizar el último mensaje procesado (last_msg_id)
        if "last_msg_id" in state:
            self.last_msg_id = state["last_msg_id"]

        logging.info(f"Replica: Estado completo cargado. Campos cargados: {list(state.keys())}")

        # ==================================================================
        # CAIDA DESPUES DE CARGAR EL ESTADO
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE CARGAR EL ESTADO"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================





