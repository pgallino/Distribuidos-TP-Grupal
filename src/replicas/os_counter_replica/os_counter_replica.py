from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class OsCounterReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para OsCounter."""
        self.counters = defaultdict(lambda: (0, 0, 0))  # Diccionario con contadores para Windows, Mac y Linux

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        # logging.info(f"OsCounterReplica: Recibiendo PushDataMessage del cliente {msg.id}")

        # Actualizar los contadores con los datos recibidos
        for client_id, (windows, mac, linux) in msg.data.items():
            self.counters[client_id] = (
                windows,
                mac,
                linux,
            )
        # logging.info(f"OsCounterReplica: Estado actualizado: {self.counters}")

    def _process_pull_data(self):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""
        logging.info(f"OsCounterReplica: Respondiendo a solicitud de PullDataMessage")

        # Crear el mensaje de respuesta con el estado actual
        response_data = PushDataMessage(data=dict(self.counters))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            self.send_queue,  # Cola de respuesta
            response_data.encode()
        )
        logging.info(f"OsCounterReplica: Estado enviado exitosamente: {self.counters}")
