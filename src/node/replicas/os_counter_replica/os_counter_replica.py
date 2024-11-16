from collections import defaultdict
import logging
import subprocess
import time
from messages.messages import PushDataMessage, PullDataMessage
from replica import Replica
from utils.constants import Q_REPLICA_MAIN, Q_REPLICA_RESPONSE


class OsCounterReplica(Replica):

    def __init__(self, id: int):
        super().__init__(id)
        self._middleware.declare_queue(Q_REPLICA_MAIN)
        self._middleware.declare_queue(Q_REPLICA_RESPONSE)
        self.inactivity_detected = False

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN, self.process_replica_message, self.ask_keepalive, 5, auto_ack=False)
        
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

    def _send_data(self, msg: PullDataMessage):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""
        logging.info(f"OsCounterReplica: Respondiendo a solicitud de PullDataMessage del cliente {msg.id}")

        # Crear el mensaje de respuesta con el estado actual
        response_data = PushDataMessage(id=self.id, data=dict(self.counters))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            Q_REPLICA_RESPONSE,  # Cola de respuesta fija
            response_data.encode()
        )
        logging.info("OsCounterReplica: Estado enviado exitosamente a Q_REPLICA_RESPONSE.")

    def ask_keepalive(self) -> bool:
        # manda por la cola de keepalive un keepalive
        # espera a recibir el keepalive
        self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN, self.consume_alive, self._handle_inactive_master, 5, auto_ack=False)
        # si recibe keepalive --> termina la funcion (devuelve true)
        # si hay timeout --> hay que ejecutar algorimto de consenso 
        return

    def consume_alive(self):
        # procesar el coso de alive
        pass

    def _handle_inactive_master(self):
        """Levanta un nuevo contenedor para reemplazar al maestro inactivo utilizando subprocess."""
        container_name = "os_counter_1"  # Reemplaza con el nombre de tu contenedor

        try:
            # Reiniciar el contenedor maestro
            result = subprocess.run(['docker', 'start', container_name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info('Docker start executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout.decode(), result.stderr.decode()))

        except Exception as e:
            logging.error(f"Error inesperado al manejar el maestro inactivo: {e}")
