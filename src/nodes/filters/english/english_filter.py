import logging
from typing import List, Tuple
from messages.messages import ListMessage, MsgType, decode_msg
from messages.reviews_msg import BasicReview, ReviewsType
from node import Node  # Importa la clase base Node
from utils.middleware_constants import E_FROM_PROP, K_NOTIFICATION, Q_ENGLISH_Q4_JOINER, Q_NOTIFICATION, Q_Q4_JOINER_ENGLISH, Q_TO_PROP
import langid
from utils.utils import NodeType


class EnglishFilter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], container_name):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, container_name, n_next_nodes=n_next_nodes)

        # Configura las colas y los intercambios específicos para EnglishFilter
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_Q4_JOINER_ENGLISH)

        self._middleware.declare_queue(Q_TO_PROP)
        self.notification_queue = Q_NOTIFICATION + f'_{container_name}_{id}'
        self._middleware.declare_queue(self.notification_queue)
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(self.notification_queue, E_FROM_PROP, key=K_NOTIFICATION+f'_{container_name}')

    def get_type(self):
        return NodeType.ENGLISH

    def get_keys(self):
        return [('', 1)]

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        while not self.shutting_down:
            try:
                logging.info("Empiezo a consumir de la cola de DATA")
                self._middleware.receive_from_queue(Q_Q4_JOINER_ENGLISH, self._process_message, auto_ack=False)
                # Empieza a escuchar por la cola de notificaciones
                self._middleware.receive_from_queue(self.notification_queue, self._process_notification, auto_ack=False)

            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                    self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_SCORE_ENGLISH."""
        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.REVIEWS:
            self._process_reviews_message(msg)

        elif msg.type == MsgType.FIN:
            self._process_fin_message(ch, method, msg.client_id)
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_reviews_message(self, msg):
        """Filtra y envía reseñas en inglés a la cola correspondiente."""
        en_reviews = [
            BasicReview(review.app_id) for review in msg.items if self.is_english(review.text)
        ]

        if en_reviews:
            english_reviews_msg = ListMessage(type=MsgType.REVIEWS, item_type= ReviewsType.BASICREVIEW, items=en_reviews, client_id=msg.client_id)
            self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, english_reviews_msg.encode())

    def is_english(self, text):
        """Detecta si el texto está en inglés usando langid."""
        lang, _ = langid.classify(text)
        return lang == 'en'  # Retorna True si el idioma detectado es inglés
