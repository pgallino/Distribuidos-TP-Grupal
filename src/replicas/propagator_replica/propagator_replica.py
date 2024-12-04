import logging
from messages.messages import PushDataMessage
from replica import Replica
from utils.middleware_constants import E_FROM_PROP, K_FIN
from utils.utils import NodeType

class PropagatorReplica(Replica):

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Propagator."""
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(self.recv_queue, E_FROM_PROP, key=K_FIN+'.#')
        self.nodes_fins_state = {}
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.PROPAGATOR_REPLICA

    def _create_pull_answer(self):
        response_data = PushDataMessage( data={
            "nodes_fins_state": self.nodes_fins_state,
            "last_msg_id": self.last_msg_id
        }, node_id=self.id)
        return response_data

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        update = msg.data

        # Identificar el tipo de actualización
        update_type = update.get("type")
        client_id = update.get("id")

        if update_type == "new_client":
            self._new_client(client_id, update.get("update", {}))
        elif update_type == "node_fin_state":
            self._node_fin_state(client_id, update.get("update", {}))
        elif update_type == "delete":
            self._delete_client_state(client_id)
        else:
            logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

    def _node_fin_state(self, client_id: int, update):
        """Actualiza los juegos de un cliente en la réplica."""
        node_type, node_instance, value = update
        
        if client_id in self.nodes_fins_state:
            self.nodes_fins_state[client_id][node_type][node_instance] = value

        logging.info(f"Estado actualizado de cliente {client_id} para {node_type} {node_instance}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        if client_id in self.nodes_fins_state:
            del self.nodes_fins_state[client_id]

    def _new_client(self, client_id: int, update: dict):
        if not client_id in self.nodes_fins_state:
            self.nodes_fins_state[client_id] = update

    def _process_fin_message(self, msg):
        client_id = msg.client_id
        try:
            node = NodeType(msg.node_type)
        except:
            logging.warning(f"No existe enum de NodeType para valor {msg.node_type}")

        if client_id in self.nodes_fins_state:
            self.nodes_fins_state[client_id][node.name]['fins_propagated'] += 1
        
        logging.info(f"actualicé con el fin en: cliente {client_id} de {node.name} --> fins_propagated {self.nodes_fins_state[client_id][node.name]['fins_propagated']}")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar contadores de sistemas operativos por cliente
        if 'nodes_fins_state' in state:
            self.nodes_fins_state = state['nodes_fins_state']

        # Actualizar el último mensaje procesado
        if "last_msg_id" in state:
            self.last_msg_id = state.get('last_msg_id')

        logging.info(f"estado actualizado a: {self.nodes_fins_state}, con last_id {self.last_msg_id}")