from messages.messages import decode_msg, MsgType, Result, QueryNumber
from middleware.middleware import Middleware
import logging

Q_TRIMMER_OS_COUNTER = "trimmer-os_counter"
Q_QUERY_RESULT_1 = "query_result_1"
E_TRIMMER_FILTERS = 'trimmer-filters'
K_GAME = 'game'

class OsCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_OS_COUNTER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)
        self._middleware.bind_queue(Q_TRIMMER_OS_COUNTER, E_TRIMMER_FILTERS, K_GAME)

        self._middleware.declare_queue(Q_QUERY_RESULT_1)

        self.counters = {}

    def run(self):
        # self.logger.custom("action: listen_to_queue")
        while True:
            # self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_TRIMMER_OS_COUNTER)
            msg = decode_msg(raw_message[4:])
            if msg.type == MsgType.GAME:
                client_counters = self.counters.get(msg.id, (0, 0, 0))
                windows, mac, linux = client_counters
                if msg.windows: windows += 1
                if msg.mac: mac += 1
                if msg.linux: linux += 1
                self.counters[msg.id] = (windows, mac, linux)
            # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
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