import logging
from messages.messages import PushDataMessage, ResultMessage, decode_msg, MsgType
from messages.results_msg import Q1Result, QueryNumber

from node import Node
from utils.middleware_constants import E_FROM_TRIMMER, K_Q1GAME, Q_QUERY_RESULT_1, Q_TRIMMER_OS_COUNTER
from utils.utils import NodeType, log_with_location, simulate_random_failure

class OsCounter(Node):

    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name)

        self.n_replicas = n_replicas

        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_FROM_TRIMMER, K_Q1GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self.os_count = {}
        self.last_msg_id = 0

    def run(self):

        try:
            
            if self.n_replicas > 0:

                # ==================================================================
                # CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS
                simulate_random_failure(self, log_with_location("CAIDA ANTES DE SINCRONIZAR CON LAS REPLICAS"))
                # ==================================================================
                self._synchronize_with_replicas()  # Sincronizar con la réplica al inicio
            
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER, self._process_message, auto_ack=False)

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

        # ==================================================================
        # CAIDA ANTES DE HACER EL ACK AL MENSAJE
        simulate_random_failure(self, log_with_location("CAIDA ANTES DE HACER EL ACK AL MENSAJE"))
        # ==================================================================
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # ==================================================================
        # CAIDA DESPUES DE HACER EL ACK AL MENSAJE
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE HACER EL ACK AL MENSAJE"))
        # ==================================================================

    
    def get_type(self):
        return NodeType.OS_COUNTER
    
    def _process_game_message(self, msg):
        # Actualizar los contadores
        client_counters = self.os_count.get(msg.client_id, (0, 0, 0))
        windows, mac, linux = client_counters

        for game in msg.items:
            if game.windows:
                windows += 1
            if game.mac:
                mac += 1
            if game.linux:
                linux += 1

        # Guardar los contadores actualizados
        self.os_count[msg.client_id] = (windows, mac, linux)

        # Enviar los datos actualizados a la réplica

        # ==================================================================
        # CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y ANTES DE ENVIAR A LA REPLICA
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y ANTES DE ENVIAR A LA REPLICA"))
        # ==================================================================

        self.push_update('os_count', msg.client_id, self.os_count[msg.client_id])

        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido

        # ==================================================================
        # CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y DESPUES DE ENVIAR A LA REPLICA
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE ACTUALIZAR LOS CONTADORES Y ANTES DE ENVIAR A LA REPLICA"))
        # ==================================================================


    def _process_fin_message(self, msg):

        # Obtener el contador final para el id del mensaje
        if msg.client_id in self.os_count:
            windows_count, mac_count, linux_count = self.os_count[msg.client_id]

            # Crear el mensaje de resultado
            q1_result = Q1Result(windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)
            result_message = ResultMessage(client_id=msg.client_id, result_type=QueryNumber.Q1, result=q1_result)

            # Enviar el mensaje codificado a la cola de resultados}
            # TODO: Como no es atomico esto y el ACK, podria mandar repetido un resultado al dispatcher
            # TODO: Descartar mensajes repetidos en el dispatcher

            # ==================================================================
            # CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y ANTES DE ENVIARLO
            simulate_random_failure(self, log_with_location("CAIDA DESPUES DE CREAR EL MENSAJE DE RESULTADO Y ANTES DE ENVIARLO"))
            # ==================================================================
            self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())

        del self.os_count[msg.client_id]
        self.push_update('delete', msg.client_id)

    def load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""

        # ==================================================================
        # CAIDA ANTES DE CARGAR EL ESTADO
        simulate_random_failure(self, log_with_location("CAIDA ANTES DE CARGAR EL ESTADO"))
        # ==================================================================

        state = msg.data

        # Actualizar contadores de sistemas operativos por cliente
        if "os_count" in state:
            for client_id, (windows, mac, linux) in state["os_count"].items():
                self.os_count[client_id] = (windows, mac, linux)
            logging.info(f"Replica: Contadores de sistemas operativos actualizados desde estado recibido.")

        # ==================================================================
        # CAIDA DESPUES DE CARGAR EL ESTADO
        simulate_random_failure(self, log_with_location("CAIDA DESPUES DE CARGAR EL ESTADO"))
        # ==================================================================

        # Actualizar el último mensaje procesado
        if "last_msg_id" in state:
            self.last_msg_id = state["last_msg_id"]