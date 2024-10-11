import signal
from messages.messages import decode_msg, MsgType
from messages.results_msg import Q1Result
from middleware.middleware import Middleware
import logging
import traceback

from utils.constants import E_TRIMMER_FILTERS, K_Q1GAME, Q_QUERY_RESULT_1, Q_TRIMMER_OS_COUNTER

class OsCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_TRIMMER_FILTERS, K_Q1GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self.counters = {}

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.channel.stop_consuming()
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)

            if msg.type == MsgType.GAMES:
                # Actualizar los contadores para cada sistema operativo
                client_counters = self.counters.get(msg.id, (0, 0, 0))
                windows, mac, linux = client_counters

                for game in msg.games:
                    if game.windows:
                        windows += 1
                    if game.mac:
                        mac += 1
                    if game.linux:
                        linux += 1

                # Guardar los contadores actualizados
                self.counters[msg.id] = (windows, mac, linux)

            elif msg.type == MsgType.FIN:
                # Obtener el contador final para el id del mensaje
                if msg.id in self.counters:
                    windows_count, mac_count, linux_count = self.counters[msg.id]

                    # Crear el mensaje de resultado
                    result_message = Q1Result(id=msg.id, windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)

                    # Enviar el mensaje codificado a la cola de resultados
                    self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())
                
                # Cierra la conexión y marca el cierre en proceso
                self.shutting_down = True
                self._middleware.connection.close()

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER, process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                traceback.print_exc()
