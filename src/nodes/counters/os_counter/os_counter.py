import logging
from typing import List, Tuple
from messages.messages import PushDataMessage, ResultMessage, decode_msg, MsgType
from messages.results_msg import Q1Result, QueryNumber

from node import Node
from utils.constants import E_FROM_PROP, E_FROM_TRIMMER, K_FIN, K_Q1GAME, Q_QUERY_RESULT_1, Q_TRIMMER_OS_COUNTER

class OsCounter(Node):

    def __init__(self, id: int, n_nodes: int, container_name: str, n_replicas: int):
        super().__init__(id, n_nodes, container_name)

        self.n_replicas = n_replicas

        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_FROM_TRIMMER, K_Q1GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self._middleware.declare_exchange(E_FROM_PROP)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_FROM_PROP, key=K_FIN+f'_{container_name}')

        self.counters = {}

    def run(self):

        try:
            
            if self.n_replicas > 0:
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
            self.ch_for_fin = ch
            self.fin_to_ack = method.delivery_tag
            self._middleware.channel.stop_consuming()
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    def _process_game_message(self, msg):
        # Actualizar los contadores
        client_counters = self.counters.get(msg.client_id, (0, 0, 0))
        windows, mac, linux = client_counters

        for game in msg.items:
            if game.windows:
                windows += 1
            if game.mac:
                mac += 1
            if game.linux:
                linux += 1

        # Guardar los contadores actualizados
        self.counters[msg.client_id] = (windows, mac, linux)

        # Enviar los datos actualizados a la réplica

        self.push_update('os_count', msg.client_id, self.counters[msg.client_id])

        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido


    def _process_fin_message(self, msg):

        # Obtener el contador final para el id del mensaje
        if msg.client_id in self.counters:
            windows_count, mac_count, linux_count = self.counters[msg.client_id]

            # Crear el mensaje de resultado
            q1_result = Q1Result(windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)
            result_message = ResultMessage(client_id=msg.client_id, result_type=QueryNumber.Q1, result=q1_result)

            # Enviar el mensaje codificado a la cola de resultados}
            # TODO: Como no es atomico esto y el ACK, podria mandar repetido un resultado al dispatcher
            # TODO: Descartar mensajes repetidos en el dispatcher
            self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())

        del self.counters[msg.client_id]
        self.push_update('delete', msg.client_id)

    def load_state(self, msg: PushDataMessage):
        for client_id, (windows, mac, linux) in msg.data.items():
            self.counters[client_id] = (windows, mac, linux)