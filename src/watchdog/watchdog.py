# Clase watchdog
import logging
import signal
from multiprocessing import Process
import socket
import time
from messages.messages import decode_msg
from utils.utils import reanimate_container, recv_msg
from election.election_manager import ElectionManager


class WatchDog:
    def __init__(self, id: int, n_watchdogs: int, container_name: str, n_nodes_instances: list = [], check_interval=5):
        """
        Inicializa el WatchDog.
        
        :param id: Identificador del WatchDog.
        :param n_watchdogs: Número total de watchdogs.
        :param container_name: Nombre del contenedor a monitorear.
        :param n_nodes_instances: Lista de tuplas con tipos de nodos y sus instancias.
        :param check_interval: Intervalo en segundos para verificar los nodos.
        """
        time.sleep(10)
        self.id = id
        self.n_watchdogs = n_watchdogs
        self.container_name = container_name
        self.check_interval = check_interval
        self.shutting_down = False
        self.listener_process = None
        self.nodes_state = {}

        self._set_nodes_state(n_nodes_instances)

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """
        Inicia el proceso principal del WatchDog.
        - Inicia el listener en un proceso separado.
        - Periódicamente verifica si los nodos están vivos.
        """
        self.init_listener_process()

        # if leader == self.id:
        while not self.shutting_down:
            try:
                for node_type, instances in self.nodes_state.items():
                    for instance_id, is_alive in instances.items():
                        if not self.shutting_down:
                            self._check_node(node_type, instance_id)
                time.sleep(self.check_interval)  # Esperar antes de la próxima verificación
            except Exception as e:
                logging.error(f"WatchDog {self.id}: Error en el proceso de verificación: {e}")
        # TODO: Que haya varios watchdogs, si se cae el lider (el que vigila los nodos)
        #       se inicia eleccion del lider, que se va a encargar de seguir vigilando
        #       los nodos y de levantar el watchdog que origino la eleccion de lider.
        # else:
        #     while not self.shutting_down:
        #         try:
        #             self._check_node('watchdog', leader) # type: ignore
        #             time.sleep(self.check_interval)  # Esperar antes de la próxima verificación
        #         except Exception as e:
        #             logging.error(f"WatchDog {self.id}: Error en el proceso de verificación: {e}")
        #     pass

    def _check_node(self, node_type: str, instance_id: int):
        """
        Verifica si un nodo está vivo enviando un mensaje de conexión.
        
        :param node_type: Tipo del nodo.
        :param instance_id: ID de la instancia del nodo.
        """
        node_address = f"{node_type}_{instance_id}"
        port = 12345  # Con este puerto se puede aprovechar el KA que ya tienen los nodos para las replicas
        
        try:
            with socket.create_connection((node_address, port), timeout=1):
                logging.info(f"WatchDog {self.id}: KEEP_ALIVE enviado a {node_address}:{port}.")
                self.update_node_state(node_type, instance_id, True)  # Nodo está vivo
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"WatchDog {self.id}: Nodo {node_address}:{port} no responde.")
            #TODO que reanimate retorne si lo logro o no
            self.update_node_state(node_type, instance_id, False)  # Nodo está muerto
            if reanimate_container(node_address):
                self.update_node_state(node_type, instance_id, True)  # Nodo está vivo
        except Exception as e:
            logging.error(f"WatchDog {self.id}: Error inesperado al verificar nodo {node_address}:{port}: {e}")
            self.update_node_state(node_type, instance_id, False)

    def _shutdown(self):
        """Gracefully shuts down the node, stopping consumption and closing connections."""
        if self.shutting_down:
            return
        
        logging.info("action: shutdown_node | result: in progress...")
        self.shutting_down = True

        if self.listener_process:
            self.listener_process.terminate()
            self.listener_process.join()
        logging.info("action: shutdown_node | result: success")

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()

    def _set_nodes_state(self, n_nodes_instances):
        """
        Configura el estado inicial de los nodos.

        `nodes_state` será un diccionario donde cada clave es un tipo de nodo y
        su valor es otro diccionario que contiene `True` o `False` para indicar
        si las instancias están vivas o muertas.
        """
        for node_type, instances in n_nodes_instances:
            # Inicializa cada tipo de nodo con sus instancias en estado `False` (muertas)
            self.nodes_state[node_type] = {instance: False for instance in range(1, instances+1)}

        # Imprimir el estado resultante
        logging.info(f"ME QUEDO ASI EL STATE: {self.nodes_state}")

    def update_node_state(self, node_type: str, instance_id: int, is_alive: bool):
        """
        Actualiza el estado de un nodo específico.

        :param node_type: Tipo de nodo (por ejemplo, "GENRE", "SCORE").
        :param instance_id: ID de la instancia del nodo.
        :param is_alive: Nuevo estado del nodo (`True` para vivo, `False` para muerto).
        """
        if node_type in self.nodes_state:
            if instance_id in self.nodes_state[node_type]:
                self.nodes_state[node_type][instance_id] = is_alive
                logging.info(f"Updated state for {node_type} instance {instance_id} to {'alive' if is_alive else 'dead'}.")
            else:
                logging.warning(f"Instance ID {instance_id} not found for node type {node_type}.")
        else:
            logging.warning(f"Node type {node_type} not found in nodes_state.")


    def init_listener_process(self):
        process = Process(
            target=handle_listener_process,
            args=['watchdog', self.id, 12345, self.n_watchdogs])
        process.start()
        self.listener_process = process

    def set_leader(self, leader_id):
        self.leader = leader_id


def handle_listener_process(container_name, id, port, node_ids):

    def handle_sigterm(sig, frame):
        """Handle SIGTERM signal to close the node gracefully."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        listener_socket.close()
        shutting_down = True

    shutting_down = False
    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener_socket.bind((f'{container_name}_{id}', port))
        listener_socket.listen(len(node_ids))
        logging.info(f"node {id}: Escuchando en el puerto {port}.")

        while True:
            conn, _ = listener_socket.accept()
            raw_msg = recv_msg(conn)
            msg = decode_msg(raw_msg)
            # procesar mensaje

            conn.close()
    except Exception as e:
        if not shutting_down:
            logging.error(f"node {id}: Error iniciando el servidor socket: {e}")
            listener_socket.close()