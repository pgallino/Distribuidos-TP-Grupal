# Clase base Nodo
import logging
import random
import signal
from multiprocessing import Process, Value, Condition
import socket
import time
from middleware.middleware import Middleware
from coordinator import CoordinatorNode
from messages.messages import MsgType, NodeType, PushDataMessage, SimpleMessage, decode_msg
from utils.constants import E_FROM_MASTER_PUSH, Q_REPLICA_MASTER, Q_TO_PROP
from utils.listener import NodeListener
from utils.utils import recv_msg

PROB_FALLA = 0.2

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
        self.fin_to_ack = None

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
    
    def get_type(self) -> NodeType:
        raise NotImplementedError("Debe implementarse en las subclases")

    def _receive_message(self, queue_name, callback):
        raise NotImplementedError("Debe implementarse en las subclases")
    
    def _process_fin_message(self, ch, method, client_id: int):
        logging.info(f'Llego un FIN del cliente {client_id}')
        fin_notify_msg = SimpleMessage(type=MsgType.FIN_NOTIFICATION, client_id=client_id, node_type=self.get_type().value, node_instance=self.id)
        self._middleware.send_to_queue(Q_TO_PROP, fin_notify_msg.encode())
        if random.random() < PROB_FALLA:
            logging.warning(f"Se cae justo despues de mandarle al propagator el FIN del cliente {client_id}")
            self._shutdown()
            logging.info("Voy a exitiar con CODE 0")
            exit(0)
        self.fin_to_ack = (client_id, ch, method.delivery_tag)
        ch.stop_consuming()
    
    def _process_notification(self, ch, method, properties, raw_message):
        """Callback para procesar las notificaciones de fins propagados"""
        msg = decode_msg(raw_message)

        if random.random() < PROB_FALLA:
            logging.warning(f"Se cae justo esperando la noti de la propagacion del FIN cliente {self.fin_to_ack[0]}")
            self._shutdown()
            logging.info("Voy a exitiar con CODE 0")
            exit(0)

        if msg.type == MsgType.FIN_PROPAGATED:
            if self.fin_to_ack:
                client_id, fin_ch, tag = self.fin_to_ack
                if msg.client_id == client_id:
                    logging.info(f'Llego notificacion de FIN propagado para el cliente {client_id}')
                    fin_ch.basic_ack(delivery_tag=tag)
                    self.fin_to_ack = None
                    ch.stop_consuming()
                    if random.random() < PROB_FALLA:
                        logging.warning(f"Se cae justo despues de hacer ack del FIN cliente {client_id}")
                        self._shutdown()
                        logging.info("Voy a exitiar con CODE 0")
                        exit(0)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
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
        # Declarar las colas necesarias
        self.push_exchange_name = E_FROM_MASTER_PUSH + f'_{self.container_name}_{self.id}'
        self.replica_queue = Q_REPLICA_MASTER + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.push_exchange_name, type="fanout") # -> exchange para broadcast de push y pull
        self._middleware.declare_queue(self.replica_queue) # -> cola para recibir respuestas

        # Función de callback para procesar la respuesta
        def on_replica_response(ch, method, properties, body):
            msg = decode_msg(body)
            if isinstance(msg, PushDataMessage):
                self.load_state(msg)
                logging.info(f"action: Sincronizado con réplica | Datos recibidos: {msg.data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logging.info("Callback terminado.")
            ch.stop_consuming()

        # Enviar un mensaje `PullDataMessage`
        pull_msg = SimpleMessage(type=MsgType.PULL_DATA)
        self._middleware.send_to_queue(self.push_exchange_name, pull_msg.encode())
        self._middleware.receive_from_queue(self.replica_queue, on_replica_response, auto_ack=False)          
        self.connected = 1

    def push_update(self, type: str, client_id: int, update = None):

        if self.n_replicas > 0:
            if update:
                data = {'type': type, 'id': client_id, 'update': update}
            else:
                data = {'type': type, 'id': client_id}

            push_msg = PushDataMessage(data=data)
            self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())


def init_listener(id, ip_prefix, connected):
    listener = NodeListener(id, ip_prefix, connected)
    listener.run()


def create_coordinator(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, processing_client, condition):
    coordinator = CoordinatorNode(id, queue_name, exchange_name, n_nodes, keys, keys_exchange, processing_client, condition)
    coordinator._listen_coordination_queue()
    