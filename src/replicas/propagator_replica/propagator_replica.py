import logging
import threading
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from replica import Replica
from utils.middleware_constants import E_FROM_PROP, K_FIN
from utils.utils import NodeType


class PropagatorReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

        # Declarar colas y exchanges específicos de Propagator
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(self.recv_queue, E_FROM_PROP, key=K_FIN + '.#')

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Propagator."""
       # Inicialización de almacenamiento
        self.nodes_fins_state = {}

    def get_type(self):
        return NodeType.PROPAGATOR_REPLICA

    def _create_pull_answer(self):
        response_data = PushDataMessage(
            data={
                "nodes_fins_state": self.nodes_fins_state,
                "last_msg_id": self.last_msg_id,
            },
            node_id=self.id
        )
        return response_data

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        if msg.msg_id > self.last_msg_id or msg.msg_id == 0:
            update = msg.data
            update_type = update.get("type")
            client_id = update.get("id")

            with self.lock:
                if update_type == "new_client":
                    self._new_client(client_id, update.get("update", {}))
                elif update_type == "node_fin_state":
                    self._node_fin_state(client_id, update.get("update", {}))
                elif update_type == "delete":
                    self._delete_client_state(client_id)
                else:
                    logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

                self.last_msg_id = msg.msg_id
                self.synchronized = True

    def _node_fin_state(self, client_id: int, update):
        """Actualiza el estado de finalización de nodos para un cliente."""
        node_type, node_instance, value = update

        if client_id in self.nodes_fins_state:
            self.nodes_fins_state[client_id][node_type][node_instance] = value

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        if client_id in self.nodes_fins_state:
            del self.nodes_fins_state[client_id]

    def _new_client(self, client_id: int, update: dict):
        """Añade un nuevo cliente al estado."""
        if client_id not in self.nodes_fins_state:
            self.nodes_fins_state[client_id] = update

    def _process_fin_message(self, msg):
        """Procesa un mensaje de finalización (FIN) para un cliente."""

        if msg.msg_id > self.last_msg_id or msg.msg_id == 0:
            client_id = msg.client_id
            try:
                node = NodeType(msg.node_type)
            except ValueError:
                logging.warning(f"No existe enum de NodeType para valor {msg.node_type}")
                return
            
            with self.lock:
                if client_id in self.nodes_fins_state:
                    self.nodes_fins_state[client_id][node.name]['fins_propagated'] += 1
                
                self.last_msg_id = msg.msg_id
                self.synchronized = True

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        if 'nodes_fins_state' in state:
            self.nodes_fins_state = state['nodes_fins_state']

        if "last_msg_id" in state:
            self.last_msg_id = state.get('last_msg_id')

        self.synchronized = True

        logging.info(f"Estado actualizado a: {self.nodes_fins_state}, con last_msg_id {self.last_msg_id}")