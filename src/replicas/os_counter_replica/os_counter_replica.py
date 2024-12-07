from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica
from utils.utils import NodeType

class OsCounterReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""

        self.os_count = defaultdict(lambda: defaultdict(int))
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.OS_COUNTER_REPLICA

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        if msg.msg_id > self.last_msg_id or msg.msg_id == 0:
            state = msg.data
            update_type = state.get("type")
            client_id = state.get("id")

            with self.lock:
                if update_type == "os_count":
                    updated_counters = state.get("update", {})
                    self.os_count[client_id] = updated_counters

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
                "os_count": dict(self.os_count)
            },
            node_id=self.id
        )
        return response_data

    def _delete_client_state(self, client_id):
        """Elimina el estado de un cliente específico."""
        if client_id in self.os_count:
            del self.os_count[client_id]
            logging.info(f"Replica: Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data
        with self.lock:
            if "os_count" in state:
                for client_id, counters in state["os_count"].items():
                    self.os_count[client_id] = counters

            if "last_msg_id" in state:
                self.last_msg_id = state["last_msg_id"]

            self.synchronized = True
            logging.info(f"Replica: Estado cargado. Último mensaje ID: {self.last_msg_id}")
