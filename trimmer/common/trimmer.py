from messages.messages import decode_msg, MSG_TYPE_HANDSHAKE, MSG_TYPE_DATA, MSG_TYPE_FIN, GAME_CSV, REVIEW_CSV
from middleware.middleware import Middleware
import logging

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
E_TRIMMER_FILTERS = 'trimmer-filters'
K_GAME = 'game'
K_REVIEW = 'review'


#### ESTANDAR NOMBRE COLAS ####
# Q_ORIGEN_DESTINO = "origen-destino"

#### ESTANDAR NOMBRE EXCHANGES ####
# E_FROM_ORIGEN = "from_origen"

#### ESTANDAR NOMBRE CLAVES ####
# K_GAME = "game"

class Trimmer:
    def __init__(self):
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)

    def run(self):

        while True:
            logging.warning("action: listening_queue | result: in_progress")
            raw_message = self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER)
            msg = decode_msg(raw_message[2:])
            logging.warning(f"action: listening_queue | result: success | msg: {msg}")
            if msg.type == MSG_TYPE_DATA:
                logging.warning("action: sending_data | result: in_progress")
                key = K_GAME if msg.dataset == GAME_CSV else K_REVIEW
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=key)
                logging.warning(f"action: sending_data | result: success | data sent to {key}")
            elif msg.type == MSG_TYPE_FIN:
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GAME)
                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                self._middleware.connection.close()
                return