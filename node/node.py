# Clase base Nodo
import logging
from multiprocessing import Process, Value, Condition
import signal
from middleware.middleware import Middleware
from coordinator import CoordinatorNode
from messages.messages import CoordFin


class Node:
    def __init__(self, id: int, n_nodes: int, n_next_nodes: list):
        """
        Base class for nodes to avoid code repetition.

        Parameters:
        - id: Unique identifier for the node.
        - n_nodes: Total number of nodes in the system.
        - n_next_nodes: List of tuples with next node details (node type, count).
        """
        self.id = id
        self.n_nodes = n_nodes
        self.n_next_nodes = n_next_nodes
        self.shutting_down = False
        self._middleware = Middleware()
        self.logger = logging.getLogger(__name__)
        self.coordination_process = None
        self.condition = Condition()
        self.processing_client = Value('i', -1)  # 'i' indica un entero

        # lado del nodo
        # with self.condition:
        #     cambio la condicion
        #     self.condition.notify_all()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _shutdown(self):
        """Gracefully shuts down the node, stopping consumption and closing connections."""
        if self.shutting_down:
            return

        self.logger.custom("action: shutdown | result: in progress...")
        self.shutting_down = True

        if self.coordination_process and self.coordination_process.is_alive():
            self.coordination_process.terminate()
            self.coordination_process.join()

        # Cierra la conexión de manera segura
        self._middleware.close()
        self.logger.custom("action: shutdown | result: success")

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        self.logger.info("Received SIGTERM, shutting down gracefully.")
        self._shutdown()

    def _setup_coordination_queue(self, queue_prefix, exchange_name):
        """Sets up the coordination queue if there are multiple nodes."""

    def run(self):
        raise NotImplementedError("Debe implementarse en las subclases")

    def _receive_message(self, queue_name, callback):
        raise NotImplementedError("Debe implementarse en las subclases")
    
    def forward_coordfin(self, coord_exchange_name, msg):
        for node_id in range(1, self.n_nodes + 1):
            if node_id != self.id:
                # Se reenvia el CoordFin del Fin del cliente a cada otra instancia del nodo
                self._middleware.send_to_queue(coord_exchange_name, CoordFin(msg.id, self.id).encode(), key=f"{node_id}")

    def init_coordinator(self, id: int, queue_name: str, exchange_name: str, n_nodes: int, keys, keys_exchange: str):
        coordinator = CoordinatorNode(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, self.processing_client, self.condition)
        process = Process(target=coordinator._listen_coordination_queue)
        process.start()
        self.coordination_process = process

    