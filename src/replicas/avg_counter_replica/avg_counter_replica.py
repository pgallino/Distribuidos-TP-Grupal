from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica
from utils.utils import NodeType

class AvgCounterReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""
        # Inicialización de almacenamiento
        self.avg_count = defaultdict(list)

        # Variables de estado compartido
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.AVG_COUNTER_REPLICA

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        if msg.msg_id > self.last_msg_id or msg.msg_id == 0:
            state = msg.data
            update_type = state.get("type")
            client_id = state.get("id")

            with self.lock:
                if update_type == "avg_count":
                    heap_data = state.get("update", [])
                    self.avg_count[client_id] = heap_data

                elif update_type == "delete":
                    self._delete_client_state(client_id)

                else:
                    logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

                self.last_msg_id = msg.msg_id
                self.synchronized = True

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        response_data = PushDataMessage(
            data={
                "last_msg_id": self.last_msg_id,
                "avg_count": {client_id: list(heap) for client_id, heap in self.avg_count.items()}
            },
            node_id=self.id
        )
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
        with self.lock:
            if "avg_count" in state:
                for client_id, heap_data in state["avg_count"].items():
                    self.avg_count[client_id] = heap_data

            if "last_msg_id" in state:
                self.last_msg_id = state["last_msg_id"]

            self.synchronized = True
            logging.info(f"Replica: Estado cargado. Último mensaje ID: {self.last_msg_id}")

