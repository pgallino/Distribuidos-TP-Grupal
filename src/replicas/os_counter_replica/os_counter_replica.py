from collections import defaultdict
import logging
from multiprocessing import Process
from messages.messages import PushDataMessage
from replica import Replica
from utils.listener import ReplicaListener
from utils.utils import NodeType

class OsCounterReplica(Replica):

    def get_type(self):
            return NodeType.OS_COUNTER_REPLICA
    
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para OsCounter."""
        self.os_count = defaultdict(lambda: (0, 0, 0))  # Diccionario con contadores para Windows, Mac y Linux
          # Último mensaje procesado

        logging.info("Replica: Almacenamiento inicializado para OsCounter.")

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""

        state = msg.data

        # Identificar el tipo de actualización
        update_type = state.get("type")
        client_id = state.get("id")

        if update_type == "os_count":
            updated_counters = state.get("update")
            if updated_counters:
                self.os_count[client_id] = updated_counters

        elif update_type == "delete":
            self._delete_client_state(client_id)

        else:
            logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

        # Actualizar last_msg_id después de procesar un mensaje válido
        self.last_msg_id = msg.msg_id

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        response_data = PushDataMessage(data={
            "last_msg_id": self.last_msg_id,
            "os_count": dict(self.os_count)  # Convierte defaultdict a dict estándar
        }, node_id=self.id)
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

        # Actualizar contadores de sistemas operativos por cliente
        if "os_count" in state:
            for client_id, (windows, mac, linux) in state["os_count"].items():
                self.os_count[client_id] = (windows, mac, linux)
            logging.info(f"Replica: Contadores de sistemas operativos actualizados desde estado recibido.")

        # Actualizar el último mensaje procesado
        if "last_msg_id" in state:
            self.last_msg_id = state.get('last_msg_id')

        logging.info(f"last_id actualizado {self.last_msg_id}")

    def start_listener(self):
        self.listener = Process(target=init_listener, args=(id, self.container_name, self.os_count, self.lock, self.port,))
        self.listener.start()


def init_listener(id, container_name, port, state, lock_state):
    listener = ReplicaListener(id, container_name, state, lock_state, port, )
    listener.run()