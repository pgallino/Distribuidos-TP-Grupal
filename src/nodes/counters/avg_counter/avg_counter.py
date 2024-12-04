from collections import defaultdict
import logging
from messages.messages import MsgType, PushDataMessage, ResultMessage, decode_msg
from messages.results_msg import Q2Result, QueryNumber
import heapq

from node import Node
from utils.container_constants import ENDPOINTS_PROB_FAILURE
from utils.middleware_constants import E_FROM_PROP, K_FIN, Q_RELEASE_DATE_AVG_COUNTER, Q_QUERY_RESULT_2
from utils.utils import NodeType, log_with_location, simulate_random_failure

class AvgCounter(Node):

    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name)

        self.n_replicas = n_replicas
        self._middleware.declare_queue(Q_RELEASE_DATE_AVG_COUNTER)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(Q_RELEASE_DATE_AVG_COUNTER, E_FROM_PROP, key=K_FIN+f'.{container_name}')

        # Diccionario para almacenar un heap por cada cliente
        self.avg_count = defaultdict(list)  # client_id -> heap
        self.last_msg_id = 0

    def get_type(self) -> NodeType:
        return NodeType.AVG_COUNTER

    def run(self):

        try:

            if self.n_replicas > 0:
                # ==================================================================
                # CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS
                simulate_random_failure(self, log_with_location("CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS"), probability=ENDPOINTS_PROB_FAILURE)
                # ==================================================================
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
            logging.info(f"Llego un FIN de cliente {msg.client_id}")
            self._process_fin_message(msg)

        # ==================================================================
        # CAIDA ANTES DE HACER EL ACK AL MENSAJE
        # simulate_random_failure(self, log_with_location("CAIDA ANTES DE HACER EL ACK AL MENSAJE"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # ==================================================================
        # CAIDA DESPUES DE HACER EL ACK AL MENSAJE
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE HACER EL ACK AL MENSAJE"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================
    
    def _process_game_message(self, msg):
        client_id = msg.client_id  # Asumo que cada mensaje tiene un client_id

        # Obtener el heap para este cliente
        client_heap = self.avg_count[client_id]

        for game in msg.items:
            if len(client_heap) < 10:
                heapq.heappush(client_heap, (game.avg_playtime, game.app_id, game.name))
            elif game.avg_playtime > client_heap[0][0]:  # 0 es el índice de avg_playtime
                heapq.heapreplace(client_heap, (game.avg_playtime, game.app_id, game.name))

        # ==================================================================
        # CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y ANTES DE ENVIAR A LA REPLICA
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y ANTES DE ENVIAR A LA REPLICA"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        # Enviar los datos actualizados a la réplica
        self.push_update('avg_count', client_id, client_heap)

        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido

        # ==================================================================
        # CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y DESPUES DE ENVIAR A LA REPLICA
        # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y DESPUES DE ENVIAR A LA REPLICA ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================
    
    def _process_fin_message(self, msg):

        client_id = msg.client_id  # Usar el client_id del mensaje FIN
        # TODO: Hacer el push:Fin

        if client_id in self.avg_count:
            # Obtener el heap del cliente y ordenarlo
            client_heap = self.avg_count[client_id]
            result = sorted(client_heap, key=lambda x: x[0], reverse=True)
            top_games = [(name, avg_playtime) for avg_playtime, _, name in result]

            # Crear y enviar el mensaje de resultado
            q2_result = Q2Result(top_games=top_games)
            result_message = ResultMessage(client_id=msg.client_id, result_type=QueryNumber.Q2, result=q2_result)

            # ==================================================================
            # CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y ANTES DE ENVIARLO
            simulate_random_failure(self, log_with_location("CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y ANTES DE ENVIARLO"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            self._middleware.send_to_queue(Q_QUERY_RESULT_2, result_message.encode())

            # ==================================================================
            # CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y DESPUES DE ENVIARLO
            # simulate_random_failure(self, log_with_location("⚠️ CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y DESPUES DE ENVIARLO ⚠️"), probability=ENDPOINTS_PROB_FAILURE)
            # ==================================================================

            # TODO: Como no es atomico esto y el ACK, podria mandar repetido un resultado al dispatcher
            # TODO: Descartar mensajes repetidos en el dispatcher
            
            # Limpiar el heap para este cliente
            del self.avg_count[client_id]
        self.push_update('delete', msg.client_id)

    def load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""

        # ==================================================================
        # CAIDA ANTES DE CARGAR EL ESTADO
        simulate_random_failure(self, log_with_location("CAIDA ANTES DE CARGAR EL ESTADO"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        state = msg.data

        # Actualizar heaps por cliente
        if "avg_count" in state:
            for client_id, heap_data in state["avg_count"].items():
                self.avg_count[client_id] = [tuple(item) for item in heap_data]
            logging.info(f"Replica: Heaps de promedio actualizados desde estado recibido.")

        # ==================================================================
        # CAIDA DESPUES DE CARGAR EL ESTADO
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE CARGAR EL ESTADO"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        # Actualizar el último mensaje procesado
        if "last_msg_id" in state:
            self.last_msg_id = state["last_msg_id"]
