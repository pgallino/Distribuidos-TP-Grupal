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
        """Sincroniza el estado con las réplicas compañeras."""
        self.push_exchange_name = E_FROM_MASTER_PUSH + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.push_exchange_name, type="fanout")
        self.recv_queue = self._middleware.declare_anonymous_queue(E_FROM_REPLICA_PULL)

        responses = {}  # Almacenar las respuestas de las réplicas
        most_recent_msg_id = self.last_msg_id

        # Función de callback para procesar las respuestas de ASK_LAST_MSG_ID
        def on_ask_response(ch, method, properties, body):
            nonlocal responses
            msg = decode_msg(body)
            if msg.type == MsgType.ANS_LAST_MSG_ID:
                logging.info(f"Master: Recibido last_msg_id={msg.msg_id} de réplica {msg.node_id}.")
                responses[msg.node_id] = msg.msg_id
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # Detener consumo si ya se recibieron todas las respuestas
            if len(responses) >= self.n_replicas:
                ch.stop_consuming()

        # Enviar mensaje ASK_LAST_MSG_ID a todas las réplicas
        ask_msg = SimpleMessage(type=MsgType.ASK_LAST_MSG_ID)
        self._middleware.send_to_queue(self.push_exchange_name, ask_msg.encode())
        logging.info("Master: Mensaje ASK_LAST_MSG_ID enviado a todas las réplicas.")

        # Recibir respuestas
        self._middleware.receive_from_queue(self.recv_queue, on_ask_response, auto_ack=False)

        # Verificar si se recibieron respuestas
        if not responses:
            logging.warning("Master: No se recibieron respuestas de las réplicas. Abortando sincronización.")
            return

        # Determinar el nodo con el estado más actualizado
        most_recent_msg_id = max(responses.values())

        # Solicitar el estado completo
        self._request_full_state(most_recent_msg_id)

    def _request_full_state(self, target_msg_id):
        """Solicita el estado completo al nodo más actualizado."""
        pull_data_msg = SimpleMessage(type=MsgType.PULL_DATA, msg_id=target_msg_id)
        self._middleware.send_to_queue(self.push_exchange_name, pull_data_msg.encode())
        logging.info(f"Master: Mensaje PULL_DATA enviado solicitando last_msg_id={target_msg_id}.")

        def on_pull_response(ch, method, properties, body):
            msg = decode_msg(body)
            if isinstance(msg, PushDataMessage):
                if msg.data["last_msg_id"] == target_msg_id:
                    self.load_state(msg)
                    logging.info(f"Master: Estado sincronizado con last_msg_id={target_msg_id} desde réplica {msg.msg_id}.")
                    ch.stop_consuming()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # Recibir la respuesta del estado completo
        self._middleware.receive_from_queue(self.recv_queue, on_pull_response, auto_ack=False)


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
    