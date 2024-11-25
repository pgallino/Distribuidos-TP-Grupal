import logging
from typing import List, Tuple
from messages.messages import PushDataMessage, ResultMessage, decode_msg, MsgType
from messages.results_msg import Q1Result, QueryNumber

from node import Node
from utils.constants import E_FROM_TRIMMER, K_Q1GAME, Q_QUERY_RESULT_1, Q_TRIMMER_OS_COUNTER

class OsCounter(Node):

    def __init__(self, id: int, n_nodes: int, container_name: str):
        super().__init__(id=id, n_nodes=n_nodes, container_name=container_name)

        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_FROM_TRIMMER, K_Q1GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self.counters = {}

    def run(self):

        try:

            # TODO: Verificar si hay que enviarle el resultado a algun Client
            self.init_ka(self.container_name)
            self._synchronize_with_replica()  # Sincronizar con la réplica al inicio
            
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
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    
    def _process_game_message(self, msg):
        # Actualizar los contadores
        client_counters = self.counters.get(msg.id, (0, 0, 0))
        windows, mac, linux = client_counters

        for game in msg.items:
            if game.windows:
                windows += 1
            if game.mac:
                mac += 1
            if game.linux:
                linux += 1

        # Guardar los contadores actualizados
        self.counters[msg.id] = (windows, mac, linux)

        # Enviar los datos actualizados a la réplica
        data = {msg.id: self.counters[msg.id]}
        push_msg = PushDataMessage(data=data)

        # TODO: Como no es atómico puede romper justo despues de enviarlo a la replica y no hacer el ACK
        # TODO: Posible Solucion: Ids en los mensajes para que si la replica recibe repetido lo descarte
        # TODO: Opcion 2: si con el delivery_tag se puede chequear si se recibe un mensaje repetido
        self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())


    def _process_fin_message(self, msg):

        # TODO: Hacer el push:Fin

        # Obtener el contador final para el id del mensaje
        if msg.id in self.counters:
            windows_count, mac_count, linux_count = self.counters[msg.id]

            # Crear el mensaje de resultado
            q1_result = Q1Result(windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)
            result_message = ResultMessage(id=msg.id, result_type=QueryNumber.Q1, result=q1_result)

            # Enviar el mensaje codificado a la cola de resultados}
            # TODO: Como no es atomico esto y el ACK, podria mandar repetido un resultado al dispatcher
            # TODO: Descartar mensajes repetidos en el dispatcher
            self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())

        # TODO: Hacer el push:Delete
        del self.counters[msg.id]

    def load_state(self, msg: PushDataMessage):
        for client_id, (windows, mac, linux) in msg.data.items():
            self.counters[client_id] = (windows, mac, linux)