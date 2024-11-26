from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class OsCounterReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para OsCounter."""
        self.state = defaultdict(lambda: (0, 0, 0))  # Diccionario con contadores para Windows, Mac y Linux

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        # logging.info(f"OsCounterReplica: Recibiendo PushDataMessage del cliente {msg.client_id}")

        # Actualizar los contadores con los datos recibidos
        for client_id, (windows, mac, linux) in msg.data.items():
            self.state[client_id] = (
                windows,
                mac,
                linux,
            )
        # logging.info(f"OsCounterReplica: Estado actualizado: {self.state}")

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        # Identificar el tipo de actualización
        update_type = state.get("type")
        client_id = state.get("id")

        if update_type == "os_count":
            updated_counters = state.get("update")
            if updated_counters:
                self.state[client_id] = updated_counters

                # logging.info(f"Estado actualizado para {client_id}: {self.state[client_id]}")

        elif update_type == "delete":
            self._delete_client_state(client_id)
        else:
            logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

    def _delete_client_state(self, client_id):
        """Elimina el estado de un cliente específico."""
        if client_id in self.state:
            del self.state[client_id]
            logging.info(f"Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")