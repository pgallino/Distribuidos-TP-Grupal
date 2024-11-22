from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class AvgCounterReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para AvgCounter."""
        self.state = defaultdict(list)  # Diccionario para almacenar un heap por cliente

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        # logging.info(f"AvgCounterReplica: Recibiendo PushDataMessage del cliente {msg.id}")

        # Actualizar los heaps con los datos recibidos
        for client_id, heap_data in msg.data.items():
            self.state[client_id] = heap_data
        # logging.info(f"AvgCounterReplica: Estado actualizado: {self.state}")
