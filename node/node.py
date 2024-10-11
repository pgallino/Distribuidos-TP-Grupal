# Clase base Nodo
import logging
import signal
from middleware.middleware import Middleware


class Node:
    def __init__(self, id: int, n_nodes: int, n_next_nodes: list):
        """
        Base class for nodes to avoid code repetition.

        Parameters:
        - id: Unique identifier for the node.
        - n_nodes: Total number of nodes in the system.
        - n_next_nodes: List of tuples with next node details (node type, count).
        - middleware: Middleware instance for message handling.
        """
        self.id = id
        self.n_nodes = n_nodes
        self.n_next_nodes = n_next_nodes
        self.shutting_down = False
        self._middleware = Middleware()
        self.fins_counter = 0  # Common fin counter for coordinating shutdown.
        self.logger = logging.getLogger(__name__)

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _shutdown(self):
        """Gracefully shuts down the node, closing connections."""
        if self.shutting_down:
            return
        self.shutting_down = True
        self._middleware.channel.stop_consuming()
        self._middleware.connection.close()

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        self.logger.info("Received SIGTERM, shutting down gracefully.")
        self._shutdown()

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
            self.fins_counter = 1

    def run(self):
        raise NotImplementedError("Debe implementarse en las subclases")

    def _receive_message(self, queue_name, callback):
        raise NotImplementedError("Debe implementarse en las subclases")
    
    def _process_fin(self, ch, method, properties, raw_message):
        raise NotImplementedError("Debe implementarse en las subclases")
