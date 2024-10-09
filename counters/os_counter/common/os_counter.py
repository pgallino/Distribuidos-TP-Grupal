import signal
from messages.messages import decode_msg, MsgType, Result, QueryNumber
from middleware.middleware import Middleware
import logging

Q_TRIMMER_OS_COUNTER = "trimmer-os_counter"
Q_QUERY_RESULT_1 = "query_result_1"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_Q1GAME = 'q1game'

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
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                # self.logger.custom('action: listening_queue | result: in_progress')
                raw_message = self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER)
                msg = decode_msg(raw_message)
                if msg.type == MsgType.GAMES:
                    for game in msg.games:
                        client_counters = self.counters.get(msg.id, (0, 0, 0))
                        windows, mac, linux = client_counters
                        if game.windows:
                            windows += 1
                        if game.mac:
                            mac += 1
                        if game.linux:
                            linux += 1
                        self.counters[msg.id] = (windows, mac, linux)
                if msg.type == MsgType.FIN:
                    # Crear el mensaje de resultado
                    counter = self.counters[msg.id]

                    result_text = "OS Count Summary:\n"
                    result_text += f"Windows: {counter[0]}\n"
                    result_text += f"Mac: {counter[1]}\n"
                    result_text += f"Linux: {counter[2]}\n"

                    # Crear el mensaje Result y enviarlo a la cola de resultados
                    result_message = Result(id=msg.id, query_number=QueryNumber.Q1.value, result=result_text)
                    self._middleware.send_to_queue(Q_QUERY_RESULT_1, result_message.encode())

                    # self.logger.custom(f"action: shutting_down | result: in_progress")
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return
        
        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")