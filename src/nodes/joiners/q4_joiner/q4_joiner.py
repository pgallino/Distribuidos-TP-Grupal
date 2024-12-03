from collections import defaultdict
import logging
from typing import List, Tuple
from messages.messages import ListMessage, MsgType, ResultMessage, decode_msg, PushDataMessage
from messages.results_msg import Q4Result, QueryNumber
from messages.reviews_msg import ReviewsType, TextReview
from node import Node

from utils.constants import E_FROM_PROP, E_FROM_SCORE, K_FIN, K_NEGATIVE_TEXT, Q_SCORE_Q4_JOINER, Q_Q4_JOINER_ENGLISH, E_FROM_GENRE, K_SHOOTER_GAMES, Q_ENGLISH_Q4_JOINER, Q_GENRE_Q4_JOINER, Q_QUERY_RESULT_4

class Q4Joiner(Node):

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], batch_size: int, n_reviews: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name, n_next_nodes=n_next_nodes)

        self.n_replicas = n_replicas
        self.batch_size = batch_size * 1024
        self.n_reviews = n_reviews
        
        self._middleware.declare_queue(Q_GENRE_Q4_JOINER)
        self._middleware.declare_queue(Q_SCORE_Q4_JOINER)
        self._middleware.declare_queue(Q_Q4_JOINER_ENGLISH)
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_QUERY_RESULT_4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_GENRE, K_SHOOTER_GAMES)
        self._middleware.bind_queue(Q_SCORE_Q4_JOINER, E_FROM_SCORE, K_NEGATIVE_TEXT)

        self._middleware.declare_exchange(E_FROM_PROP)
        self._middleware.bind_queue(Q_GENRE_Q4_JOINER, E_FROM_PROP, key=K_FIN+f'_{container_name}_games') # hay que modificarlo en el propagator
        self._middleware.bind_queue(Q_SCORE_Q4_JOINER, E_FROM_PROP, key=K_FIN+f'_{container_name}_reviews')
        self._middleware.bind_queue(Q_ENGLISH_Q4_JOINER, E_FROM_PROP, key=K_FIN+f'_{container_name}_english')

        # Estructuras de almacenamiento
        self.negative_reviews_count_per_client = defaultdict(lambda: defaultdict(int))  # Contará reseñas negativas en inglés, para cada cliente
        self.games_per_client = defaultdict(lambda: {})  # Detalles de juegos de acción/shooter
        self.negative_reviews_per_client = defaultdict(lambda: defaultdict(lambda: ([], False))) # Guarda las reviews negativas de los juegos
        self.fins_per_client = defaultdict(lambda: [False, False]) #primer valor corresponde al fin de juegos, y el segundo al de reviews

    def run(self):

        try:
            if self.n_replicas > 0: # verifico si se instanciaron replicas
                self._synchronize_with_replicas()

            # Consumir mensajes de ambas colas con sus respectivos callbacks en paralelo
            self._middleware.receive_from_queues([(Q_GENRE_Q4_JOINER, self.process_game_message), (Q_SCORE_Q4_JOINER, self.process_review_message), (Q_ENGLISH_Q4_JOINER, self.process_negative_review_message)], auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()

    def process_game_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_GENRE_Q4_JOINER`."""
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
            logging.info(f"Llego FIN GAMES de cliente {msg.client_id}")
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[0] = True

            self.push_update('fins', msg.client_id, client_fins)

            if client_fins[0] and client_fins[1]:
                # TODO: Mucho cuidado aca que ya envia reviews a la cola del english
                #       Hay que ver que pasa si se cae justo antes de entrar, en el
                #       medio del envio, o si se cae justo despues
                self.send_reviews(msg.client_id) # (!!!!!!!!!!!) QUE HAGA ESTO SOLAMENTE CUANDO LE LLEGA EL FIN DE REVIEWS
                # Manda el fin a los english filters
                for _ in range(self.n_next_nodes[0][1]):
                    self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, msg.encode())
        
        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_review_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_ENGLISH_Q4_JOINER`."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            
            # Inicializar diccionario de actualizaciones
            update = {}
            client_reviews = self.negative_reviews_per_client[msg.client_id]
            client_games = self.games_per_client[msg.client_id]
            games_fin_received = self.fins_per_client[msg.client_id][0]
            for review in msg.items: # para un TextReview en TextReviews
                if (not games_fin_received) or review.app_id in client_games:
                    # Debe funcionar appendiendo el elemento directamente de esta manera
                    game_reviews, _ = client_reviews[review.app_id]
                    game_reviews.append(review.text)
                    if len(game_reviews) > self.n_reviews:
                        # TODO: Mucho cuidado aca que ya envia reviews a la cola del english
                        #       Hay que ver que pasa si se cae justo antes de entrar, en el
                        #       medio del envio, o si se cae justo despues
                        self.send_reviews_v2(msg.client_id, review.app_id, game_reviews)
                        client_reviews[review.app_id] = ([], True)
                    update[review.app_id] = client_reviews[review.app_id]

            self.push_update('reviews', msg.client_id, update)

        elif msg.type == MsgType.FIN:
            logging.info(f"Llego FIN REVIEWS de cliente {msg.client_id}")
            client_fins = self.fins_per_client[msg.client_id]
            client_fins[1] = True

            self.push_update('fins', msg.client_id, client_fins)

            if client_fins[0] and client_fins[1]:
                # TODO: Mucho cuidado aca que ya envia reviews a la cola del english
                #       Hay que ver que pasa si se cae justo antes de entrar, en el
                #       medio del envio, o si se cae justo despues
                # Se termina de mandar las reviews que superan las 5000
                self.send_reviews(msg.client_id)
                # Manda el fin a los english filters
                for _ in range(self.n_next_nodes[0][1]):
                    self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, msg.encode())

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_reviews_v2(self, client_id, app_id, reviews):
        # manda las reviews del juego al filtro de ingles
        reviews_batch = []
        curr_reviews_batch_size = 0

        for review in reviews:
            text_review = TextReview(app_id, review)
            text_review_size = len(text_review.encode())
            if text_review_size + curr_reviews_batch_size > self.batch_size:
                text_reviews = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.TEXTREVIEW, items=reviews_batch, client_id=client_id)
                self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
                curr_reviews_batch_size = 0
                reviews_batch = []
            curr_reviews_batch_size += text_review_size
            reviews_batch.append(text_review)

        # si me quedaron afuera    
        if reviews_batch:
            text_reviews = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.TEXTREVIEW, items=reviews_batch, client_id=client_id)
            self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())

    def send_reviews(self, client_id):
        client_reviews = self.negative_reviews_per_client[client_id]

        # Revisa que juego tiene mas de 5000 resenia negativas
        for app_id, (reviews, overpass_threshold) in client_reviews.items():
            if overpass_threshold or len(reviews) > self.n_reviews:
                # manda las reviews del juego al filtro de ingles
                reviews_batch = []
                curr_reviews_batch_size = 0
                for review in reviews:
                    text_review = TextReview(app_id, review)
                    text_review_size = len(text_review.encode())
                    if text_review_size + curr_reviews_batch_size > self.batch_size:
                        text_reviews = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.TEXTREVIEW, items=reviews_batch, client_id=client_id)
                        self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())
                        curr_reviews_batch_size = 0
                        reviews_batch = []
                    curr_reviews_batch_size += text_review_size
                    reviews_batch.append(text_review)

                # si me quedaron afuera    
                if reviews_batch:
                    text_reviews = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.TEXTREVIEW, items=reviews_batch, client_id=client_id)
                    self._middleware.send_to_queue(Q_Q4_JOINER_ENGLISH, text_reviews.encode())

        # Borro el diccionario de textos de reviews del cliente
        del self.negative_reviews_per_client[client_id]
        
        client_reviews.clear() #limpio el diccionario
                            
    def process_negative_review_message(self, ch, method, properties, raw_message):

        msg = decode_msg(raw_message)


        if msg.type == MsgType.REVIEWS:

            # Inicializar diccionario de actualizaciones
            update = {}
            client_reviews_count = self.negative_reviews_count_per_client[msg.client_id]
            client_games = self.games_per_client[msg.client_id]
            for review in msg.items: # para un TextReview en TextReviews
                if review.app_id in client_games:
                    client_reviews_count[review.app_id] += 1
                    update[review.app_id] = client_reviews_count[review.app_id]

            self.push_update('reviews_count', msg.client_id, update)

        elif msg.type == MsgType.FIN:
            logging.info(f"Llego FIN ENGLISH de cliente {msg.client_id}")
            # TODO: Enviar a las replicas la recepcion de este FIN.
            self.join_results(msg.client_id)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def join_results(self, client_id: int):

        client_reviews_count = self.negative_reviews_count_per_client[client_id]
        client_games = self.games_per_client[client_id]

        # Filtrar juegos de acción con más de 5,000 reseñas negativas en inglés
        negative_reviews = sorted(
            [
                (app_id, client_games[app_id], count)
                for app_id, count in client_reviews_count.items()
                if count > self.n_reviews
            ],
            key=lambda x: x[0], # Ordenar por app_id
            reverse=False  # Orden descendente
        )[:25]  # Tomar los 25 primeros


        # Crear y enviar el mensaje Q4Result
        q4_result = Q4Result(negative_reviews=negative_reviews)
        result_message = ResultMessage( client_id=client_id, result_type=QueryNumber.Q4, result=q4_result)
        self._middleware.send_to_queue(Q_QUERY_RESULT_4, result_message.encode())

        # Borro los diccionarios de clientes ya resueltos
        del self.games_per_client[client_id]
        del self.negative_reviews_count_per_client[client_id]

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

        # Actualizar cantidad de reseñas negativas por cliente
        if "negative_reviews_count_per_client" in state:
            for client_id, reviews in state["negative_reviews_count_per_client"].items():
                if client_id not in self.negative_reviews_count_per_client:
                    self.negative_reviews_count_per_client[client_id] = defaultdict(int)
                self.negative_reviews_count_per_client[client_id].update(reviews)
            logging.info(f"Replica: Cantidad de reseñas negativas actualizadas desde estado recibido.")

        # Actualizar reseñas negativas por cliente
        if "negative_reviews_per_client" in state:
            for client_id, negative_reviews in state["negative_reviews_per_client"].items():
                if client_id not in self.negative_reviews_per_client:
                    self.negative_reviews_per_client[client_id] = defaultdict(lambda: ([], False))
                self.negative_reviews_per_client[client_id].update(negative_reviews)
            logging.info(f"Replica: Reseñas negativas actualizadas desde estado recibido.")

        # Actualizar fins por cliente
        if "fins_per_client" in state:
            for client_id, fins in state["fins_per_client"].items():
                self.fins_per_client[client_id] = fins
            logging.info(f"Replica: Estados FIN actualizados desde estado recibido.")

        logging.info(f"Replica: Estado completo cargado. Campos cargados: {list(state.keys())}")