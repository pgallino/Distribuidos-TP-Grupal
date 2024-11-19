from collections import defaultdict
import logging
import socket
import signal
import subprocess
from messages.messages import PushDataMessage
from replica import Replica
from utils.constants import Q_REPLICA_MAIN, Q_REPLICA_RESPONSE

TIMEOUT = 5

class OsCounterReplica(Replica):

    def __init__(self, id: int):
        super().__init__(id)
        self._middleware.declare_queue(Q_REPLICA_MAIN)
        self._middleware.declare_queue(Q_REPLICA_RESPONSE)

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        
        while True:
            self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN, self.process_replica_message, TIMEOUT, auto_ack=False)
            self.ask_keepalive()

        
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

    def _send_data(self):
        """Codifica el estado actual y envía una respuesta a `Q_REPLICA_RESPONSE`."""
        logging.info(f"OsCounterReplica: Respondiendo a solicitud de PullDataMessage")

        # Crear el mensaje de respuesta con el estado actual
        response_data = PushDataMessage(data=dict(self.counters))

        # Enviar el mensaje a Q_REPLICA_RESPONSE
        self._middleware.send_to_queue(
            Q_REPLICA_RESPONSE,  # Cola de respuesta fija
            response_data.encode()
        )
        logging.info("OsCounterReplica: Estado enviado exitosamente a Q_REPLICA_RESPONSE.")

    def _process_fin(self):
        self._shutdown()

    def ask_keepalive(self):
        # ===== Opcion con cola =====
        # manda por la cola de keepalive un keepalive
        # espera a recibir el keepalive
        # self._middleware.receive_from_queue_with_timeout(Q_REPLICA_MAIN, self.consume_alive, self._handle_inactive_master, 5, auto_ack=False)
        # si recibe keepalive --> termina la funcion (devuelve true)
        # si hay timeout --> hay que ejecutar algorimto de consenso
        # ===========================
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        try: 
            logging.info("Intento conectarme")
            sock.connect(('os_counter_1', 12345))
            logging.info("me conecte, MASTER vivo")
        except ConnectionRefusedError:
            logging.error("Conexión rechazada. El servidor podría estar caído.")
            self._handle_inactive_master()
        except socket.gaierror as e:
            logging.error(f"Error de resolución del nombre de host: {e}")
            logging.info("Posible problema de red o DNS. Revisar configuración de Docker.")
            self._handle_inactive_master()
        except socket.timeout:
            logging.error("Timeout al intentar conectar con el servidor.")
            self._handle_inactive_master()
        except Exception as e:
            logging.error(f"Error inesperado durante la conexión del socket: {e}")
            if not self.shutting_down:
                logging.info(f"Error inesperado en run: {e}")
            self._handle_inactive_master()
        sock.close()

    def _handle_inactive_master(self):
        
        """
        Intenta detener y luego reiniciar un contenedor Docker, verificando su estado.
        :param container_name: Nombre del contenedor a manejar.
        """

        container_name = 'os_counter_1'
        try:

            # Verificar el estado de los contenedores
            ps_result = subprocess.run(
                ['docker', 'ps', '-a'],  # Incluye contenedores detenidos
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logging.info(
                'Docker ps executed. Result: {}. Output: {}. Error: {}'.format(
                    ps_result.returncode,
                    ps_result.stdout.decode().strip(),
                    ps_result.stderr.decode().strip()
                )
            )

            # Intentar reiniciar el contenedor
            start_result = subprocess.run(
                ['docker', 'start', container_name],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logging.info(
                'Docker start executed. Result: {}. Output: {}. Error: {}'.format(
                    start_result.returncode,
                    start_result.stdout.decode().strip(),
                    start_result.stderr.decode().strip()
                )
            )

            # Verificar nuevamente el estado de los contenedores
            ps_result_after = subprocess.run(
                ['docker', 'ps'],  # Solo contenedores en ejecución
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logging.info(
                'Docker ps after start executed. Result: {}. Output: {}. Error: {}'.format(
                    ps_result_after.returncode,
                    ps_result_after.stdout.decode().strip(),
                    ps_result_after.stderr.decode().strip()
                )
            )

        except Exception as e:
            logging.error(f"Unexpected error while handling inactive master: {e}")
