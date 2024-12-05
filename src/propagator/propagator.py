# Clase watchdog
import logging
import signal
from multiprocessing import Process
import socket
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.container_constants import ENDPOINTS_PROB_FAILURE
from utils.listener import PropagatorListener
from utils.utils import log_with_location, recv_msg, NodeType, simulate_random_failure
from utils.middleware_constants import E_FROM_MASTER_PUSH, E_FROM_PROP, E_FROM_REPLICA_PULL_ANS, K_FIN, K_NOTIFICATION, Q_TO_PROP

class Propagator:
    def __init__(self, id: int, container_name: str, nodes_instances: dict[str, int], n_replicas: int):
        """
        Inicializa el Propagator.
        
        :param id: Identificador del Propagator.
        :param container_name: nombre del container
        :param nodes_instances: Lista de tuplas con tipos de nodos y sus instancias.
        :param check_interval: Intervalo en segundos para verificar los nodos.
        """
        self.id = id
        self.container_name = container_name
        self.shutting_down = False
        self.listener = None
        self.n_replicas = n_replicas

        self.nodes_instances = nodes_instances
        self.nodes_fins_state = {}
        self._middleware = Middleware()

        self.last_msg_id = 0

        # Configuracion de colas
        # hacerlo con las colas para la comunicacion con replicas: E_FROM_MASTER_PUSH, Q_REPLICA_MASTER
        self._middleware.declare_queue(Q_TO_PROP)
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def get_type(self):
        return NodeType.PROPAGATOR

    def run(self):
        """
        Inicia el proceso principal del WatchDog.
        - Inicia el listener en un proceso separado.
        - Periódicamente verifica si los nodos están vivos.
        """
        self.init_listener_process()

        try:

            if self.n_replicas > 0: # verifico si se instanciaron replicas
                self._synchronize_with_replicas()
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
        # logging.info(f'Llego un una notificacion de fin cliente {msg.client_id} de {node.name} {msg.node_instance}')
        if msg.client_id not in self.nodes_fins_state:
            self._add_new_client_state(msg.client_id)

        nodes_client_fins = self.nodes_fins_state[msg.client_id][node.name]
        nodes_client_fins[msg.node_instance] = True

        # logging.info(f'Se actualiza el diccionario: {self.nodes_fins_state[msg.client_id]}')
        # pusheamos el cambio de estado a las replicas
        self.push_update('node_fin_state', msg.client_id, update=(node.name, msg.node_instance, True))
        # ==================================================================
        # CAIDA POST PUSHEAR LLEGADA DE FIN CLIENTE
        simulate_random_failure(self, log_with_location(f"CAIDA POST PUSHEAR LLEGADA DE FIN CLIENTE {msg.client_id} de {node.name} {msg.node_instance}"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

        # nos fijamos si se puede propagar el fin
        for fin_received in nodes_client_fins:
            # logging.info(f"Fin_received de {node.name} {fin_received} esta en {nodes_client_fins[fin_received]}")
            # logging.info(f"Primera condicion: {isinstance(nodes_client_fins[fin_received], bool)}")
            if not fin_received == 'fins_propagated' and not nodes_client_fins[fin_received]: # no se puede
                return
        # se puede propagar el fin
        self._propagate_fins(nodes_client_fins, msg.client_id, node)

        # ==================================================================
        # CAIDA POST PROPAGACION DE FINS DE CLIENTE
        simulate_random_failure(self, log_with_location(f"CAIDA POST PROPAGACION DE FINS DE CLIENTE {msg.client_id} DE {node.name}"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================
        
        # notificamos a los nodos que ya propagamos el fin
        # logging.info(f'Se notifica a {node.name} que ya se propagaron los fins del cliente {msg.client_id}')
        fin_propagated_msg = SimpleMessage(type=MsgType.FIN_PROPAGATED, client_id=msg.client_id, node_type=node.value)
        self._middleware.send_to_queue(E_FROM_PROP, fin_propagated_msg.encode(), key=K_NOTIFICATION + f'_{NodeType.node_type_to_string(node)}')

        # ==================================================================
        # CAIDA POST NOTIFICACION DE FINS CLIENTE
        simulate_random_failure(self, log_with_location(f"CAIDA POST NOTIFICACION DE FINS DE CLIENTE {msg.client_id} A {node.name}"), probability=ENDPOINTS_PROB_FAILURE)
        # ==================================================================

    def _process_delete_client(self, msg: SimpleMessage):
        # logging.info(f'Me llego un CLIENT_CLOSE del cliente {msg.client_id}')
        if msg.client_id in self.nodes_fins_state:
            del self.nodes_fins_state[msg.client_id]
            # pushear el cambio de estado a las replicas
            self.push_update('delete', msg.client_id)

    def _propagate_fins(self, nodes_client_fins: dict[int, bool], client_id: int, origin_node: NodeType):
        fins_propagated = nodes_client_fins['fins_propagated'] # lo consigue gracias a las replicas
        # logging.info(f'Se propagan los fins del cliente {client_id}, desde {origin_node.name}. Ya habia {fins_propagated} fins propagados')
        next_nodes = NodeType.get_next_nodes(origin_node)
        
        aggregate = 0
        for node in next_nodes:
            if node.name in self.nodes_instances:
                # logging.info(f'curr_instances = self.nodes_instances[{node.name}] = {self.nodes_instances[node.name]}')
                curr_instances = self.nodes_instances[node.name]
            else:
                # logging.info('curr_instances = 1')
                curr_instances = 1
            if aggregate + curr_instances <= fins_propagated: # ya se mandaron los fins a ese nodo
                aggregate += curr_instances
                continue
            elif aggregate < fins_propagated: # se mandaron algunos fins a ese nodo, pero no todos
                # logging.info(f'fins_to_propagate = aggregate + curr_instances - fins_propagated = {aggregate} + {curr_instances} - {fins_propagated}')
                fins_to_propagate = aggregate + curr_instances - fins_propagated
            else: # no se mando ningun fin al nodo
                # logging.info(f'fins_to_propagate = curr_instances = {curr_instances}')
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

            fin_msg = SimpleMessage(type=MsgType.FIN, client_id=client_id, node_type=origin_node.value, msg_id=self.last_msg_id)
            for _ in range(fins_to_propagate):
                # ==================================================================
                # CAIDA EN MEDIO DE PROPAGACION FINS CLIENTE
                simulate_random_failure(self, log_with_location(f"CAIDA EN MEDIO DE PROPAGACION FINS CLIENTE {client_id} de {origin_node.name}"), probability=0.001)
                # ==================================================================
                # logging.info(f"Envie fin con key {K_FIN+f'_{name}'}")
                self._middleware.send_to_queue(E_FROM_PROP, fin_msg.encode(), key=K_FIN+f'.{name}')
                self.last_msg_id += 1 # se le agrega 1
            aggregate += curr_instances

        nodes_client_fins['fins_propagated'] = aggregate
        # logging.info(f'Fins del cliente {client_id} propagados desde {origin_node.name}')
    
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
            logging.error(f"action: shutdown_node | result: ENDPOINTS_PROB_FAILUREl | error: {e}")
        
        self._middleware.check_closed()

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
        
        # logging.info(f'Se crea el diccionario {initial_nodes_states} para el cliente {client_id}')
        self.nodes_fins_state[client_id] = initial_nodes_states
        self.push_update('new_client', client_id, update=initial_nodes_states)

    def _synchronize_with_replicas(self):
        """Solicita el estado a las réplicas y sincroniza el nodo."""
        # logging.info(f"Replica {self.id}: Solicitando estado a las réplicas compañeras.")

        self.push_exchange_name = E_FROM_MASTER_PUSH + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.push_exchange_name, type="fanout")
        self.pull_exchange_name = E_FROM_REPLICA_PULL_ANS + f'_{self.container_name}_{self.id}'
        self._middleware.declare_exchange(self.pull_exchange_name)
        self.recv_queue = self._middleware.declare_anonymous_queue(self.pull_exchange_name)

        # Enviar un PULL_DATA a todas las réplicas
        pull_msg = SimpleMessage(type=MsgType.PULL_DATA)
        self._middleware.send_to_queue(self.push_exchange_name, pull_msg.encode())
        # logging.info(f"Master {self.id}: Mensaje PULL_DATA enviado a todas las réplicas.")

        def on_response(ch, method, properties, body):
            """Callback para manejar las respuestas de las réplicas."""
            msg = decode_msg(body)

            if isinstance(msg, PushDataMessage):
                # logging.info(f"Master {self.id}: RECIBI PULL: de {msg.node_id}. con {msg.data}")
                self.load_state(msg)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.stop_consuming()

        # Escuchar respuestas hasta recibir de todas las réplicas
        # TODO: ver si hacer reintentos
        # TODO: ver inactivity_time
        self._middleware.receive_from_queue_with_timeout(self.recv_queue, on_response, inactivity_time=5, auto_ack=False)
        # Eliminar la cola anonima después de procesar el mensaje
        self._middleware.delete_queue(self.recv_queue)

    def load_state(self, msg: PushDataMessage):
        self.nodes_fins_state = msg.data["nodes_fins_state"]
        self.last_msg_id = msg.data["last_msg_id"]
        logging.info(f"Estado sicronizado a {self.nodes_fins_state}")

    def push_update(self, type: str, client_id: int, update = None):

        if self.n_replicas > 0:
            if update:
                data = {'type': type, 'id': client_id, 'update': update}
            else:
                data = {'type': type, 'id': client_id}

            push_msg = PushDataMessage(data=data, msg_id=self.last_msg_id)
            self._middleware.send_to_queue(self.push_exchange_name, push_msg.encode())

        self.last_msg_id += 1

    def init_listener_process(self):
        process = Process(target=init_listener, args=(self.id, 'propagator',))
        process.start()
        self.listener = process


def init_listener(id, ip_prefix):
    listener = PropagatorListener(id, ip_prefix)
    listener.run()