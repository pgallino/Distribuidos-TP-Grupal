import logging
from multiprocessing import Process
from typing import List, Tuple
from messages.messages import Alive, PullDataMessage, PushDataMessage, decode_msg, MsgType
from middleware.middleware import Middleware
from messages.results_msg import Q1Result

from node import Node
from utils.constants import E_TRIMMER_FILTERS, K_Q1GAME, Q_QUERY_RESULT_1, Q_REPLICA_MAIN, Q_REPLICA_RESPONSE, Q_TRIMMER_OS_COUNTER, Q_KEEP_ALIVE, K_KEEP_ALIVE, K_ALIVE, E_KEEP_ALIVE

class OsCounter(Node):

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        super().__init__(id, n_nodes, n_next_nodes)

        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_TRIMMER_FILTERS, K_Q1GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self.counters = {}

    def run(self):

        try:
            self._synchronize_with_replica()  # Sincronizar con la réplica al inicio

            self.init_coordinator()
            
            # Ejecuta el consumo de mensajes con el callback `process_message`
            logging.info(f"ASI QUEDO MI COUNTER DESPUES DE SINCRONIZAR: {self.counters}")
            self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER, self._process_message, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()


    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar el mensaje de la cola."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            self._process_game_message(msg)
        
        elif msg.type == MsgType.FIN:
            logging.info("LLEGE AL FIN")
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def _process_game_message(self, msg):
        # Actualizar los contadores
        client_counters = self.counters.get(msg.id, (0, 0, 0))
        windows, mac, linux = client_counters

        for game in msg.games:
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
        push_msg = PushDataMessage(id=self.id, data=data)
        self._middleware.send_to_queue(Q_REPLICA_MAIN, push_msg.encode())
        # logging.info(f"OsCounter: Datos enviados a réplica: {data}")


    def _process_fin_message(self, msg):

        # Obtener el contador final para el id del mensaje
        if msg.id in self.counters:
            windows_count, mac_count, linux_count = self.counters[msg.id]

            # Crear el mensaje de resultado
            result_message = Q1Result(id=msg.id, windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)

            # Enviar el mensaje codificado a la cola de resultados
            self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())

    def _synchronize_with_replica(self):
        # Declarar las colas necesarias
        self._middleware.declare_queue(Q_REPLICA_MAIN)
        self._middleware.declare_queue(Q_REPLICA_RESPONSE)

        # Enviar un mensaje `PullDataMessage` a Q_REPLICA_MAIN
        pull_msg = PullDataMessage(id=self.id)
        self._middleware.send_to_queue(Q_REPLICA_MAIN, pull_msg.encode())

        # Escuchar la respuesta en Q_REPLICA_RESPONSE
        def on_replica_response(ch, method, properties, body):
            msg = decode_msg(body)
            if isinstance(msg, PushDataMessage):
                for client_id, (windows, mac, linux) in msg.data.items():
                    # Reemplazar directamente los campos en self.counters
                    self.counters[int(client_id)] = (windows, mac, linux)
                logging.info(f"OsCounter: Sincronizado con réplica. Datos recibidos: {msg.data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self._middleware.channel.stop_consuming()

        self._middleware.receive_from_queue(Q_REPLICA_RESPONSE, on_replica_response, auto_ack=False)

    def init_coordinator(self):
        process = Process(
            target=_handle_keep_alive)
        process.start()
        self.coordination_process = process

def _handle_keep_alive():
    """Proceso dedicado a manejar mensajes de Keep Alive."""
    middleware = Middleware()
    queue = Q_KEEP_ALIVE['os']
    exchange = E_KEEP_ALIVE['os']
    middleware.declare_queue(queue)
    middleware.declare_exchange(exchange)
    middleware.bind_queue(queue, exchange, key=K_KEEP_ALIVE)
    def process_keep_alive(ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        if isinstance(msg, MsgType.KEEP_ALIVE):
            alive_msg = Alive()
            middleware.send_to_queue(queue, alive_msg.encode())
            logging.info(f"OsCounter: Respondido ALIVE para KeepAliveMessage de {msg.id}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    middleware.receive_from_queue(queue, process_keep_alive, auto_ack=False)