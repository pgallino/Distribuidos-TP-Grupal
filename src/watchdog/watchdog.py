# Clase watchdog
import logging
import signal
from multiprocessing import Condition, Lock, Manager, Process, Value
import socket
import time
from election.election_logic import initiate_election
from messages.messages import MsgType, SimpleMessage, decode_msg
from utils.container_constants import LISTENER_PORT
from utils.utils import NodeType, reanimate_container, recv_msg
from listener.watchdog_listener import WatchDogListener

class WatchDog:
    def __init__(self, id: int, n_watchdogs: int, container_name: str, nodes_to_monitor: list[tuple[NodeType, int]] = [], check_interval=2):
        """
        Inicializa el WatchDog.
        
        :param id: Identificador del WatchDog.
        :param n_watchdogs: Número total de watchdogs.
        :param container_name: nombre del container
        :param nodes_to_monitor: Lista de tuplas con tipos de nodos y sus instancias.
        :param check_interval: Intervalo en segundos para verificar los nodos.
        """
        self.id = id
        self.n_watchdogs = n_watchdogs
        self.container_name = container_name
        self.check_interval = check_interval
        self.shutting_down = False
        self.listener_process = None

        self.nodes_state = {}
        manager = Manager()

        # Variables compartidas
        self.election_in_progress = manager.Value('b', False)  # Booleano
        self.election_condition = manager.Condition()  # Condición para sincronización
        self.waiting_ok = manager.Value('b', False)  # Booleano para esperar OKs
        self.ok_condition = manager.Condition()  # Condición para sincronización de OKs
        self.leader_id = manager.Value('i', -1)  # ID del líder (-1 indica sin líder)


        self._set_nodes_state(nodes_to_monitor)

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """
        Inicia el proceso principal del WatchDog.
        - Inicia el listener en un proceso separado.
        - Periódicamente verifica si los nodos están vivos.
        """

        self.init_listener_process()
        time.sleep(10)
        logging.info("[Main] Pase el sleep")
        self.election_leader()
        logging.info("[Main] Pase el election_leader")

        if self.leader_id.value == self.id:
            while not self.shutting_down:
                try:
                    for node_type, instances in self.nodes_state.items():
                        for instance_id, is_alive in instances.items():
                            if not self.shutting_down:
                                self._check_node(node_type, instance_id)
                    time.sleep(self.check_interval)  # Esperar antes de la próxima verificación
                except Exception as e:
                    logging.error(f"WatchDog {self.id}: Error en el proceso de verificación: {e}")
        else: 
            while not self.shutting_down:
                try:
                    if not self._check_node(NodeType.string_to_node_type(self.container_name), self.leader_id.value): # type: ignore
                        self.election_leader()
                    time.sleep(self.check_interval)  # Esperar antes de la próxima verificación
                except Exception as e:
                    logging.error(f"WatchDog {self.id}: Error en el proceso de verificación: {e}")

    def _check_node(self, node_type: NodeType, instance_id: int):
        """
        Verifica si un nodo está vivo enviando un mensaje de conexión.
        
        :param node_type: Tipo del nodo.
        :param instance_id: ID de la instancia del nodo.
        """
        node_address = f"{NodeType.node_type_to_string(node_type)}_{instance_id}"
        
        try:
            with socket.create_connection((node_address, LISTENER_PORT), timeout=1) as sock:
                sock.sendall(SimpleMessage(type=MsgType.KEEP_ALIVE, socket_compatible=True).encode())
                # logging.info(f"WatchDog {self.id}: KEEP_ALIVE enviado a {node_address}:{LISTENER_PORT}.")
                self.update_node_state(node_type, instance_id, True)  # Nodo está vivo
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"WatchDog {self.id}: Nodo {node_address}:{LISTENER_PORT} no responde.")
            #TODO que reanimate retorne si lo logro o no
            self.update_node_state(node_type, instance_id, False)  # Nodo está muerto
            if reanimate_container(node_address):
                self.update_node_state(node_type, instance_id, True)  # Nodo está vivo
                logging.info(f"Updated state for {node_type} instance {instance_id} to alive.")
        except Exception as e:
            logging.error(f"WatchDog {self.id}: Error inesperado al verificar nodo {node_address}:{LISTENER_PORT}: {e}")
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
        exit(0)

    def _set_nodes_state(self, nodes_to_monitor: list[tuple[NodeType, int]]):
        """
        Configura el estado inicial de los nodos.

        `nodes_state` será un diccionario donde cada clave es un tipo de nodo y
        su valor es otro diccionario que contiene `True` o `False` para indicar
        si las instancias están vivas o muertas.
        """
        for node_type, instances in nodes_to_monitor:
            # Inicializa cada tipo de nodo con sus instancias en estado `False` (muertas)
            self.nodes_state[node_type] = dict({instance: False for instance in range(1, instances+1)})

    def update_node_state(self, node_type: NodeType, instance_id: int, is_alive: bool):
        """
        Actualiza el estado de un nodo específico.

        :param node_type: Tipo de nodo (por ejemplo, "GENRE", "SCORE").
        :param instance_id: ID de la instancia del nodo.
        :param is_alive: Nuevo estado del nodo (`True` para vivo, `False` para muerto).
        """
        if node_type in self.nodes_state:
            if instance_id in self.nodes_state[node_type]:
                self.nodes_state[node_type][instance_id] = is_alive
            else:
                logging.warning(f"Instance ID {instance_id} not found for node type {node_type}.")
        else:
            logging.warning(f"Node type {node_type} not found in nodes_state.")

    def init_listener_process(self):
        process = Process(target=init_listener, args=(self.id, self.container_name, self.n_watchdogs, self.election_in_progress, self.election_condition, self.waiting_ok, self.ok_condition, self.leader_id,))
        process.start()
        self.listener_process = process
        logging.info("[Main] Inicializo listener")

    def election_leader(self):
        with self.election_condition:
            if self.election_in_progress.value:
                logging.info("Elección ya en proceso. Esperando a que termine...")
                self.election_condition.wait()  # Espera a que termine la elección
                return
            self.election_in_progress.value = True
            election = Process(target=initiate_election, args=(self.id, [id for id in range(1, self.n_watchdogs+1)], self.container_name, self.election_in_progress, self.election_condition, self.waiting_ok, self.ok_condition, self.leader_id,))

            election.start()
            election.join()


def init_listener(id, ip_prefix, n_watchdogs, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id):
    listener = WatchDogListener(id, ip_prefix, n_watchdogs, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id)
    listener.run()