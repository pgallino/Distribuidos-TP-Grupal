# Clase watchdog
import logging
import signal
from multiprocessing import Process
import socket
import time
from messages.messages import MsgType, NodeType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.utils import recv_msg
from utils.constants import E_FROM_MASTER_PUSH, E_FROM_PROP, K_FIN, K_NOTIFICATION, Q_REPLICA_MASTER, Q_TO_PROP

class Propagator:
    def __init__(self, id: int, container_name: str, nodes_instances: dict[str, int], check_interval=5):
        """
        Inicializa el Propagator.
        
        :param id: Identificador del Propagator.
        :param container_name: nombre del container
        :param nodes_instances: Lista de tuplas con tipos de nodos y sus instancias.
        :param check_interval: Intervalo en segundos para verificar los nodos.
        """
        self.id = id
        self.container_name = container_name
        self.check_interval = check_interval
        self.shutting_down = False
        # self.listener_process = None

        # self.manager = Manager()
        # self.nodes_state = self.manager.dict()
        # self.nodes_state_lock = Lock()

        self.nodes_instances = nodes_instances
        self.nodes_fins_state = {}
        self._middleware = Middleware()

        # Configuracion de colas
        # hacerlo con las colas para la comunicacion con replicas: E_FROM_MASTER_PUSH, Q_REPLICA_MASTER
        self._middleware.declare_queue(Q_TO_PROP)
        self._middleware.declare_exchange(E_FROM_PROP)

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """
        Inicia el proceso principal del WatchDog.
        - Inicia el listener en un proceso separado.
        - Periódicamente verifica si los nodos están vivos.
        """
        # self.init_listener_process()

        try:
            self._middleware.receive_from_queue(Q_TO_PROP, self._process_message, auto_ack=False)
            
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola"""

        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.FIN_NOTIFICATION:
            self._process_fin_notification(msg)

        elif msg.type == MsgType.CLIENT_CLOSE:
            self._process_delete_client(msg)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_fin_notification(self, msg: SimpleMessage):
        try:
            node = NodeType(msg.node_type)
        except:
            logging.warning(f"No existe enum de NodeType para valor {msg.node_type}")
        logging.info(f'Llego un una notificacion de fin cliente {msg.client_id} de {node.name} {msg.node_instance}')
        if msg.client_id not in self.nodes_fins_state:
            self._add_new_client_state(msg.client_id)

        nodes_client_fins = self.nodes_fins_state[msg.client_id][node.name]
        nodes_client_fins[msg.node_instance] = True
        # pusheamos el cambio de estado a las replicas

        # nos fijamos si se puede propagar el fin
        for fin_received in nodes_client_fins:
            if isinstance(fin_received, bool) and not fin_received: # no se puede
                return
        # se puede propagar el fin
        self._propagate_fins(nodes_client_fins, msg.client_id, node)
        
        # notificamos a los nodos que ya propagamos el fin
        logging.info(f'Se notifica a {node.name} que ya se propagaron los fins del cliente {msg.client_id}')
        fin_propagated_msg = SimpleMessage(MsgType.FIN_PROPAGATED, client_id=msg.client_id, node_type=node.value)
        self._middleware.send_to_queue(E_FROM_PROP, fin_propagated_msg.encode(), key=K_NOTIFICATION + f'_{NodeType.node_type_to_string(node)}')
        self.nodes_fins_state[msg.client_id][node.name]['were_notify'] = True

    def _process_delete_client(self, msg: SimpleMessage):
        if msg.client_id in self.nodes_fins_state:
            del self.nodes_fins_state[msg.client_id]
            # pushear el cambio de estado a las replicas

    def _propagate_fins(self, nodes_client_fins: dict[int, bool], client_id: int, origin_node: NodeType):
        logging.info(f'Se propagan los fins del cliente {client_id}, desde {origin_node.name}')
        fins_propagated = nodes_client_fins['fins_propagated'] # lo consigue gracias a las replicas
        next_nodes = NodeType.get_next_nodes(origin_node)
        
        aggregate = 0
        for node in next_nodes:
            if node in self.nodes_instances:
                curr_instances = self.nodes_instances[node.name]
            else:
                curr_instances = 1
            if aggregate + curr_instances <= fins_propagated: # ya se mandaron los fins a ese nodo
                aggregate += curr_instances
                continue
            elif aggregate < fins_propagated: # se mandaron algunos fins a ese nodo, pero no todos
                fins_to_propagate = aggregate + curr_instances - fins_propagated
            else: # no se mando ningun fin al nodo
                fins_to_propagate = curr_instances

            name = NodeType.node_type_to_string(node)
            # se fija que si va dirijido a algun joiner debe ver si es para la cola de games/reviews/reviews_ingles
            if node in [NodeType.Q3_JOINER, NodeType.Q4_JOINER, NodeType.Q5_JOINER]:
                if origin_node == NodeType.GENRE:
                    name += '_games'
                elif origin_node == NodeType.ENGLISH:
                    name += '_english'
                else:
                    name += '_reviews'

            for _ in range(fins_to_propagate):
                fin_msg = SimpleMessage(MsgType.FIN, client_id)
                self._middleware.send_to_queue(E_FROM_PROP, fin_msg.encode(), key=K_FIN+f'_{name}')
            logging.info(f"Envie fin con key {K_FIN+f'_{name}'}")
            aggregate += curr_instances

        nodes_client_fins['fins_propagated'] = aggregate
        logging.info(f'Fins del cliente {client_id} propagados desde {origin_node.name}')
    
    def _shutdown(self):
        """Gracefully shuts down the node, stopping consumption and closing connections."""
        if self.shutting_down:
            return
        
        logging.info("action: shutdown_node | result: in progress...")
        self.shutting_down = True

        # if self.listener_process:
        #     self.listener_process.terminate()
        #     self.listener_process.join()
        logging.info("action: shutdown_node | result: success")

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()
        exit(0)

    def _add_new_client_state(self, client_id: int):
        """
        Configura el estado inicial de los nodos.

        `nodes_state` será un diccionario donde cada clave es un tipo de nodo y
        su valor es otro diccionario que contiene `True` o `False` para indicar
        si las instancias están vivas o muertas. No es asi
        """
        initial_nodes_states = {}
        for name, instances in self.nodes_instances.items():
            initial_nodes_states[name] = {}
            
            for instance in range(1, instances+1):
                initial_nodes_states[name][instance] = False

            initial_nodes_states[name]['fins_propagated'] = 0
            initial_nodes_states[name]['were_notify'] = False
        
        logging.info(f'Se crea el diccionario {initial_nodes_states} para el cliente {client_id}')
        self.nodes_fins_state[client_id] = initial_nodes_states

    def update_node_state(self, node_type: NodeType, instance_id: int, is_alive: bool):
        """
        Actualiza el estado de un nodo específico.

        :param node_type: Tipo de nodo (por ejemplo, "GENRE", "SCORE").
        :param instance_id: ID de la instancia del nodo.
        :param is_alive: Nuevo estado del nodo (`True` para vivo, `False` para muerto).
        """
        with self.nodes_state_lock:
            if node_type in self.nodes_state:
                if instance_id in self.nodes_state[node_type]:
                    self.nodes_state[node_type][instance_id] = is_alive
                    # logging.info(f"Updated state for {node_type} instance {instance_id} to {'alive' if is_alive else 'dead'}.")
                else:
                    logging.warning(f"Instance ID {instance_id} not found for node type {node_type}.")
            else:
                logging.warning(f"Node type {node_type} not found in nodes_state.")

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

    def init_listener_process(self):
        process = Process(target=handle_listener_process, args=(f'propagator_{self.id}', self.id, 12345, 5, self.nodes_state, self.nodes_state_lock))
        process.start()
        self.listener_process = process


def handle_listener_process(container_name, id, port, n_conn, nodes_state, lock_nodes_state): # n_conn es la cantidad de clases que existen de nodos filtro

    #TODO: Armarle en handlesigterm y shutdown

    try:
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logging.info(f"me bindee a {container_name}")
        listener_socket.bind((container_name, port))
        listener_socket.listen(n_conn)
        logging.info(f"node {id}: Escuchando en el puerto {port}.")

        while True:
            conn, addr = listener_socket.accept()
            logging.info(f"me llego una conexion de {addr}")
            raw_msg = recv_msg(conn)
            msg = decode_msg(raw_msg)
            conn.close()
    except Exception as e:
        logging.error(f"node {id}: Error iniciando el servidor socket: {e.with_traceback()}")