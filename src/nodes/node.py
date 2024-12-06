# Clase base Nodo
import logging
import signal
from multiprocessing import Process, Value, Condition
import time
from middleware.middleware import Middleware
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from utils.middleware_constants import E_FROM_MASTER_PUSH, Q_TO_PROP, E_FROM_REPLICA_PULL_ANS
from utils.listener import NodeListener
from utils.utils import NodeType, simulate_random_failure, log_with_location
from utils.container_constants import FILTERS_PROB_FAILURE

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

        self.timestamp = time.time()  # Marca de tiempo al iniciar

        self.listener = Process(target=init_listener, args=(id, container_name, self.connected))
        self.listener.start()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _shutdown(self):
        """Gracefully shuts down the node, stopping consumption and closing connections."""
        if self.shutting_down:
            return
        
        logging.info("action: shutdown_node | result: in progress...")
        self.shutting_down = True

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
        # logging.info(f'Llego un FIN del cliente {client_id}')
        fin_notify_msg = SimpleMessage(type=MsgType.FIN_NOTIFICATION, client_id=client_id, node_type=self.get_type().value, node_instance=self.id)
        self._middleware.send_to_queue(Q_TO_PROP, fin_notify_msg.encode())
        # ==================================================================
        # CAIDA POST NOTIFICION DE FIN CLIENTE
        simulate_random_failure(self, log_with_location(f"CAIDA POST NOTIFICION DE FIN CLIENTE {client_id}"), probability=FILTERS_PROB_FAILURE)
        # ==================================================================
        self.fin_to_ack = (client_id, ch, method.delivery_tag)
        ch.stop_consuming()
    
    def _process_notification(self, ch, method, properties, raw_message):
        """Callback para procesar las notificaciones de fins propagados"""
        msg = decode_msg(raw_message)

        # ==================================================================
        # CAIDA ESPERANDO NOTIFICION DE FIN CLIENTE
        simulate_random_failure(self, log_with_location(f"CAIDA ESPERANDO NOTIFICION DE FIN CLIENTE {msg.client_id}"), probability=FILTERS_PROB_FAILURE)
        # ==================================================================

        if msg.type == MsgType.FIN_PROPAGATED:
            if self.fin_to_ack:
                client_id, fin_ch, tag = self.fin_to_ack
                if msg.client_id == client_id:
                    #logging.info(f'Llego notificacion de FIN propagado para el cliente {client_id}')
                    fin_ch.basic_ack(delivery_tag=tag)
                    self.fin_to_ack = None
                    ch.stop_consuming()
                    # ==================================================================
                    # CAIDA POST ACK DE FIN CLIENTE
                    simulate_random_failure(self, log_with_location(f"CAIDA POST ACK DE FIN CLIENTE {client_id}"), probability=FILTERS_PROB_FAILURE)
                    # ==================================================================
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def load_state(self, msg: PushDataMessage):
        raise NotImplementedError("Debe implementarse en las subclases")

    def _synchronize_with_replicas(self):

        """Solicita el estado a las réplicas y sincroniza el nodo."""
        logging.info(f"Replica {self.id}: Solicitando estado a las réplicas")

        self.push_exchange_name = E_FROM_MASTER_PUSH + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.push_exchange_name, type="fanout")
        self.pull_exchange_name = E_FROM_REPLICA_PULL_ANS + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.pull_exchange_name)
        self.recv_queue = self._middleware.declare_anonymous_queue(self.pull_exchange_name)

        # Enviar un PULL_DATA a todas las réplicas
        pull_msg = SimpleMessage(type=MsgType.PULL_DATA)
        self._middleware.send_to_queue(self.push_exchange_name, pull_msg.encode())
        logging.info(f"Master {self.id}: Mensaje PULL_DATA enviado a todas las réplicas.")

        responses = set()

        def on_response(ch, method, properties, body):
            """Callback para manejar las respuestas de las réplicas."""
            nonlocal responses
            msg = decode_msg(body)

            if not msg.node_id in responses:
                if isinstance(msg, SimpleMessage) and msg.type == MsgType.EMPTY_STATE:
                    logging.info(f"Master {self.id}: Recibido estado vacío de réplica {msg.node_id}.")
                    responses.add(msg.node_id)
                elif isinstance(msg, PushDataMessage):
                    last_msg_id = msg.data['last_msg_id']
                    logging.info(f"Master {self.id}: Recibido estado completo de réplica {msg.node_id}. last_msg_id = {last_msg_id}")
                    if msg.data["last_msg_id"] > self.last_msg_id:
                        logging.info(f"CARGUE DATA. last_msg_id = {last_msg_id}")
                        self.load_state(msg)
                    responses.add(msg.node_id)
            else:
                logging.info(f"RECIBI PULL REPETIDO DE {msg.node_id}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

            # Detener el consumo si ya se recibió una respuesta de cada réplica compañera
            if len(responses) >= self.n_replicas:
                logging.info("RECIBI TODOS LOS PULLS")
                ch.stop_consuming()

        # Escuchar respuestas hasta recibir de todas las réplicas

        self._middleware.receive_from_queue(self.recv_queue, on_response, auto_ack=False)
        # Eliminar la cola anonima después de procesar el mensaje
        self._middleware.delete_queue(self.recv_queue)

    def push_update(self, type: str, client_id: int, update = None):

        if self.n_replicas > 0:
            if update:
                data = {'type': type, 'id': client_id, 'update': update}
            else:
                data = {'type': type, 'id': client_id}

            push_msg = PushDataMessage(data=data, msg_id=self.last_msg_id)
            self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())

        # logging.info(f"envie push de id: {self.last_msg_id}")
        self.last_msg_id += 1

def init_listener(id, ip_prefix, connected):
    listener = NodeListener(id, ip_prefix, connected)
    listener.run()

    