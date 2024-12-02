# Clase base Nodo
import logging
import signal
from multiprocessing import Process, Value, Condition
import socket
from middleware.middleware import Middleware
from coordinator import CoordinatorNode
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from utils.middleware_constants import E_FROM_MASTER_PUSH, E_FROM_REPLICA_PULL, Q_REPLICA_MASTER
from utils.listener import NodeListener
from utils.utils import recv_msg


class Node:
    def __init__(self, id: int, n_nodes: int, container_name: str, n_next_nodes: list = []):
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
        self.container_name = container_name
        self.shutting_down = False
        self._middleware = Middleware()
        self.coordination_process = None
        self.condition = Condition()
        self.processing_client = Value('i', -1)  # 'i' indica un entero

        self.connected = Value('i', 0)  # 0 para False, 1 para True

        self.listener = Process(target=init_listener, args=(id, container_name, self.connected))
        self.listener.start()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _shutdown(self):
        """Gracefully shuts down the node, stopping consumption and closing connections."""
        if self.shutting_down:
            return
        
        logging.info("action: shutdown_node | result: in progress...")
        self.shutting_down = True

        if self.coordination_process:
            self.coordination_process.terminate()
            self.coordination_process.join()

        if self.listener:
            self.listener.terminate()
            self.listener.join()
        try:
            self._middleware.close()
            logging.info("action: shutdown_node | result: success")
        except Exception as e:
            logging.error(f"action: shutdown_node | result: fail | error: {e}")
        
        self._middleware.check_closed()

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()

    def _setup_coordination_queue(self, queue_prefix, exchange_name):
        """Sets up the coordination queue if there are multiple nodes."""

    def run(self):
        raise NotImplementedError("Debe implementarse en las subclases")
    
    def get_type(self):
        raise NotImplementedError("Debe implementarse en las subclases")

    def _receive_message(self, queue_name, callback):
        raise NotImplementedError("Debe implementarse en las subclases")
    
    def forward_coordfin(self, coord_exchange_name, msg):
        try:
            with socket.create_connection(('watchdog_1', 12345), timeout=2) as watchdog_socket:
                logging.info(f"Nodo {self.id}: Logro comunicarse con WatchDog 1 puerto {12345}.")
                # consultar al watchdog que instancias estan activas
                ask_msg = SimpleMessage(type=MsgType.ASK_ACTIVE_NODES, socket_compatible=True, node_type=self.get_type().value)
                watchdog_socket.sendall(ask_msg.encode())
                # recibir respuesta del watchdog con instancias activas
                raw_w_msg = recv_msg(watchdog_socket)
                if not raw_w_msg:
                    logging.error("Connection closed by watchdog.")
                w_msg = decode_msg(raw_w_msg)
                if w_msg.type == MsgType.ACTIVE_NODES:
                    active_nodes = w_msg.active_nodes
                    logging.info(f"Recibi los nodos que estan activos: {active_nodes}")
                    # setear las variables n_nodes y keys
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"Nodo {self.id}: Watchdog 1 puerto 12345 no responde.")
        for node_id in active_nodes:
            if node_id != self.id:
                # Se reenvia el CoordFin del Fin del cliente a cada otra instancia del nodo
                key = f"{node_id}"
                coord_msg = SimpleMessage(type=MsgType.COORDFIN, client_id=msg.client_id, node_id=self.id)
                self._middleware.send_to_queue(coord_exchange_name, coord_msg.encode(), key=key)

    def init_coordinator(self, id: int, queue_name: str, exchange_name: str, n_nodes: int, keys, keys_exchange: str):
        process = Process(
            target=create_coordinator,
            args=(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, self.processing_client, self.condition))
        process.start()
        self.coordination_process = process

    def load_state(self, msg: PushDataMessage):
        raise NotImplementedError("Debe implementarse en las subclases")

    def _synchronize_with_replicas(self):
        """Solicita el estado a las réplicas y sincroniza el nodo."""
        logging.info(f"Replica {self.id}: Solicitando estado a las réplicas compañeras.")

        self.push_exchange_name = E_FROM_MASTER_PUSH + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.push_exchange_name, type="fanout")
        self.recv_queue = self._middleware.declare_anonymous_queue(E_FROM_REPLICA_PULL)

        # Enviar un PULL_DATA a todas las réplicas
        pull_msg = SimpleMessage(type=MsgType.PULL_DATA)
        self._middleware.send_to_queue(self.push_exchange_name, pull_msg.encode())
        logging.info(f"Master {self.id}: Mensaje PULL_DATA enviado a todas las réplicas.")

        responses = {}  # Almacenar las respuestas recibidas (por ID de réplica)

        def on_response(ch, method, properties, body):
            """Callback para manejar las respuestas de las réplicas."""
            msg = decode_msg(body)

            if isinstance(msg, SimpleMessage) and msg.type == MsgType.EMPTY_STATE:
                logging.info(f"Master {self.id}: Recibido estado vacío de réplica {msg.node_id}.")
                responses[msg.node_id] = "empty"

            elif isinstance(msg, PushDataMessage):
                logging.info(f"Master {self.id}: Recibido estado completo de réplica {msg.node_id}.")
                if msg.data["last_msg_id"] > self.last_msg_id:
                    self.load_state(msg)
                    responses[msg.node_id] = "loaded"
                else:
                    responses[msg.node_id] = "ignored"

            ch.basic_ack(delivery_tag=method.delivery_tag)

            # Detener el consumo si ya se recibió una respuesta de cada réplica
            if len(responses) >= self.n_replicas:
                ch.stop_consuming()

        # Escuchar respuestas hasta recibir de todas las réplicas
        self._middleware.receive_from_queue(self.recv_queue, on_response, auto_ack=False)

        logging.info(f"Master {self.id}: Sincronización completada. Respuestas: {responses}")



    def push_update(self, type: str, client_id: int, update = None):

        if self.n_replicas > 0:
            if update:
                data = {'type': type, 'id': client_id, 'update': update}
            else:
                data = {'type': type, 'id': client_id}

            push_msg = PushDataMessage(data=data, msg_id=self.last_msg_id)
            self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())

        self.last_msg_id += 1


def init_listener(id, ip_prefix, connected):
    listener = NodeListener(id, ip_prefix, connected)
    listener.run()


def create_coordinator(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, processing_client, condition):
    coordinator = CoordinatorNode(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, processing_client, condition)
    coordinator._listen_coordination_queue()
    