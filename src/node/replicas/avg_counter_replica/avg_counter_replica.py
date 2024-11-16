from collections import defaultdict
import logging
from messages.messages import PushDataMessage, PullDataMessage
from replica import Replica
from utils.constants import Q_REPLICA_MAIN, Q_REPLICA_RESPONSE

class AvgCounterReplica(Replica):

    def __init__(self, id: int):
        super().__init__(id)
        self._middleware.declare_queue(Q_REPLICA_MAIN + "_avg_counter")
        self._middleware.declare_queue(Q_REPLICA_RESPONSE + "_avg_counter")

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        self._middleware.receive_from_queue(Q_REPLICA_MAIN + "_avg_counter", self.process_replica_message, auto_ack=False)

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para AvgCounter."""
        self.client_heaps = defaultdict(list)  # Diccionario para almacenar un heap por cliente

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        # Actualizar los heaps con los datos recibidos
        for client_id, heap_data in msg.data.items():
            self.client_heaps[client_id] = heap_data

    def _send_data(self, msg: PullDataMessage):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""
        logging.info(f"AvgCounterReplica: Respondiendo a solicitud de PullDataMessage del cliente {msg.id}")

        # Crear el mensaje de respuesta con el estado actual
        response_data = PushDataMessage(id=self.id, data=dict(self.client_heaps))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            Q_REPLICA_RESPONSE + "_avg_counter",  # Cola de respuesta fija
            response_data.encode()
        )
        logging.info("AvgCounterReplica: Estado enviado exitosamente a Q_REPLICA_RESPONSE.")