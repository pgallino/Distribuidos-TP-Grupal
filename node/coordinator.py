from collections import defaultdict
import logging
from messages.messages import MsgType, decode_msg, Fin
from multiprocessing import Process, Value, Condition
from middleware.middleware import Middleware


class CoordinatorNode:

    def __init__(self, id: int, queue_name: str, coord_exchange_name: str, n_nodes: int, keys, exchange: str, value, condition) -> None:
        self.id = id
        self._middleware = Middleware()
        self.n_nodes = n_nodes
        self.keys = keys
        self.keys_exchange = exchange
        self.exchange_name = coord_exchange_name
        self.condition = condition # Condition del flag
        self.processing_client = value # flag de coordination
        self._setup_coordination_queue(queue_name, coord_exchange_name)
        self.fins_counter = defaultdict(int)  # Common fin counter for coordinating shutdown.
        self.logger = logging.getLogger(__name__)
        if len(keys) > 1: # -> si es mas largo que uno es un exchange
            self._middleware.declare_exchange(self.keys_exchange)
        else: # -> si tiene uno solo es una cola
            self._middleware.declare_queue(self.keys_exchange)

    def _listen_coordination_queue(self):
        """Proceso dedicado a escuchar la cola de coordinación"""
        try:
            self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack = False)
        except Exception as e:
            self.logger.error(f"action: listen_to_coordination_queue | result: fail | error: {e}")

    def _setup_coordination_queue(self, queue_prefix, exchange_name):
        """Sets up the coordination queue if there are multiple nodes."""
        if self.n_nodes > 1:
            self.coordination_queue = queue_prefix + f"{self.id}"
            self._middleware.declare_queue(self.coordination_queue)
            self._middleware.declare_exchange(exchange_name)
            routing_key = f"{self.id}"
            self._middleware.bind_queue(self.coordination_queue, exchange_name, routing_key)

    def process_fin(self, ch, method, properties, raw_message):

        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            client_id = msg.id
            self.fins_counter[client_id] += 1  # Incrementa el contador para este client_id
            if self.fins_counter[client_id] == self.n_nodes - 1: # arranca en cero ahora
                for key, _ in self.keys:
                    self._middleware.send_to_queue(self.keys_exchange, msg.encode(), key=key)
                del self.fins_counter[client_id]

        elif msg.type == MsgType.COORDFIN:
            key = f"{msg.node_id}"
            with self.condition:
                self.condition.wait_for(lambda: self.processing_client.value != msg.id)
                self._middleware.send_to_queue(self.exchange_name, Fin(msg.id).encode(), key=key)
                self.condition.notify_all()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    