from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica
from utils.utils import NodeType

class AvgCounterReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para AvgCounter."""
        self.avg_count = defaultdict(list)  # Diccionario para almacenar un count por cliente
        self.last_msg_id = 0
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.AVG_COUNTER_REPLICA

    def _process_push_data(self, msg: PushDataMessage):
        #TODO: ES IGUAL AL DE OS_COUNTER
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        # Identificar el tipo de actualización
        update_type = state.get("type")
        client_id = state.get("id")

        if update_type == "avg_count":
            heap_data = state.get("update")
            if heap_data:
                # Actualizar el heap del cliente
                self.avg_count[client_id] = heap_data
                # logging.info(f"Replica: Estado actualizado para client_id={client_id}: {self.state['avg_count'][client_id]}")

        elif update_type == "delete":
            self._delete_client_state(client_id)

        # Actualizar last_msg_id después de procesar un mensaje válido
        self.last_msg_id = msg.msg_id
        # logging.info(f"Replica: Mensaje PUSH procesado con ID {msg.msg_id}. Estado actualizado.")

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        response_data = PushDataMessage(data={
            "last_msg_id": self.last_msg_id,
            "avg_count": dict(self.avg_count)
        }, node_id=self.id)
        return response_data

    def _delete_client_state(self, client_id):
        """Elimina el estado de un cliente específico."""
        if client_id in self.avg_count:
            del self.avg_count[client_id]
            logging.info(f"Replica: Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar heaps por cliente
        if "avg_count" in state:
            for client_id, heap_data in state["avg_count"].items():
                self.avg_count[client_id] = [tuple(item) for item in heap_data]
            logging.info(f"Replica: Heaps de promedio actualizados desde estado recibido.")

        # Actualizar el último mensaje procesado
        if "last_msg_id" in state:
            self.last_msg_id = state["last_msg_id"]