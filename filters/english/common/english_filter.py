from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging
import langid

Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
K_NEGATIVE = 'negative'
Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'

class EnglishFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_SCORE_ENGLISH)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_ENGLISH, E_FROM_SCORE, K_NEGATIVE)

    def is_english(self, text):
        # Detectar idioma usando langid
        lang, _ = langid.classify(text)
        return lang == 'en'  # Retorna True si el idioma detectado es inglés

    def run(self):
        # self.logger.custom("action: listen_to_queue")
        while True:
            raw_message = self._middleware.receive_from_queue(Q_SCORE_ENGLISH)
            msg = decode_msg(raw_message[4:])
            # self.logger.custom(f"action: listening_queue | result: success | msg: {msg}")
            if msg.type == MsgType.REVIEW:
                # Filtrar si la reseña está en inglés
                if self.is_english(msg.text):
                    self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())
                    # self.logger.custom(f"action: sending_data | result: success | text sent: {msg.text}")
            elif msg.type == MsgType.FIN:
                # Se reenvia el FIN al resto de nodos
                # self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())
                self._middleware.connection.close()
                # self.logger.custom("action: shutting_down | result: success")
                return
