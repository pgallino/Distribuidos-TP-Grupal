from collections import defaultdict
import logging
from messages.messages import MsgType, PushDataMessage, ResultMessage, decode_msg
from messages.results_msg import Q2Result, QueryNumber
import heapq

from node import Node
from utils.constants import Q_RELEASE_DATE_AVG_COUNTER, Q_QUERY_RESULT_2

class AvgCounter(Node):

    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id=id, n_nodes=n_nodes, container_name=container_name)

        self.n_replicas = n_replicas
        self._middleware.declare_queue(Q_RELEASE_DATE_AVG_COUNTER)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)

        # Diccionario para almacenar un heap por cada cliente
        self.client_heaps = defaultdict(list)  # client_id -> heap

    def run(self):

        try:

            if self.n_replicas > 0:
                self.init_ka(self.container_name)
                self._synchronize_with_replicas()  # Sincronizar con la réplica al inicio

            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_RELEASE_DATE_AVG_COUNTER, self._process_message, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: run | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar el mensaje de la cola."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            self._process_game_message(msg)

        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def _process_game_message(self, msg):
        client_id = msg.client_id  # Asumo que cada mensaje tiene un client_id

        # Obtener el heap para este cliente
        client_heap = self.client_heaps[client_id]

        for game in msg.items:
            if len(client_heap) < 10:
                heapq.heappush(client_heap, (game.avg_playtime, game.app_id, game.name))
            elif game.avg_playtime > client_heap[0][0]:  # 0 es el índice de avg_playtime
                heapq.heapreplace(client_heap, (game.avg_playtime, game.app_id, game.name))

        # Enviar los datos actualizados a la réplica
        self.push_update('avg_count', client_id, client_heap)

        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido
    
    def _process_fin_message(self, msg):

        client_id = msg.client_id  # Usar el client_id del mensaje FIN
        # TODO: Hacer el push:Fin

        if client_id in self.client_heaps:
            # Obtener el heap del cliente y ordenarlo
            client_heap = self.client_heaps[client_id]
            result = sorted(client_heap, key=lambda x: x[0], reverse=True)
            top_games = [(name, avg_playtime) for avg_playtime, _, name in result]

            # Crear y enviar el mensaje de resultado
            q2_result = Q2Result(top_games=top_games)
            result_message = ResultMessage(client_id=msg.client_id, result_type=QueryNumber.Q2, result=q2_result)

            self._middleware.send_to_queue(Q_QUERY_RESULT_2, result_message.encode())

            # Limpiar el heap para este cliente
            # TODO: Como no es atomico esto y el ACK, podria mandar repetido un resultado al dispatcher
            # TODO: Descartar mensajes repetidos en el dispatcher
            # TODO: Hacer el push:Delete
            del self.client_heaps[client_id]

            self.push_update('delete', msg.client_id)

    def load_state(self, msg: PushDataMessage):
        for client_id, heap_data in msg.data.items():
            self.client_heaps[client_id] = [tuple(item) for item in heap_data]

    def load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar juegos por cliente
        if "client_heaps" in state:
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