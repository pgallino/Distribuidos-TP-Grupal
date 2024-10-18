from collections import defaultdict
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware


class CoordinatorNode:

    def __init__(self, id: int, queue_name: str, exchange_name: str, n_nodes: int, keys) -> None:
        self.id = id
        self._middleware = Middleware()
        self.n_nodes = n_nodes
        self.keys = keys
        self.exchange_name = exchange_name
        self._setup_coordination_queue(queue_name, exchange_name)
        self.fins_counter = defaultdict(int)  # Common fin counter for coordinating shutdown.

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
            for i in range(1, self.n_nodes + 1):
                if i != self.id:
                    routing_key = f"coordination_{i}"
                    self._middleware.bind_queue(self.coordination_queue, exchange_name, routing_key)

    def process_fin(self, ch, method, properties, raw_message):

        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            client_id = msg.id
            self.fins_counter[client_id] += 1  # Incrementa el contador para este client_id
            if self.fins_counter[client_id] == self.n_nodes - 1: # arranca en cero ahora
                if self.id == 1:
                    for key, n_nodes in self.keys:
                        for _ in range(n_nodes):
                            self._middleware.send_to_queue(self.exchange_name, msg.encode(), key=key)
                
                # self._middleware.channel.stop_consuming() -> ya no dejo de consumir
                del self.fins_counter[client_id]
        ch.basic_ack(delivery_tag=method.delivery_tag)
    