from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class AvgCounterReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para AvgCounter."""
        self.state = defaultdict(list)  # Diccionario para almacenar un heap por cliente

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
                self.state[client_id] = heap_data
            # logging.info(f"Estado actualizado para {client_id}: {self.state[client_id]}")

        elif update_type == "delete":
            self._delete_client_state(client_id)
        else:
            logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

    def _delete_client_state(self, client_id):
        # TODO: es igual al de Os_counter
        """Elimina el estado de un cliente específico."""
        if client_id in self.state:
            del self.state[client_id]
            logging.info(f"Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")