import logging
from typing import List, Tuple
from messages.messages import ListMessage, MsgType, decode_msg
from messages.reviews_msg import BasicReview, ReviewsType, Score, TextReview
from node import Node  # Importa la clase base Node

from utils.container_constants import Q3_JOINER_CONTAINER_NAME, Q4_JOINER_CONTAINER_NAME, Q5_JOINER_CONTAINER_NAME
from utils.utils import NodeType
from utils.middleware_constants import E_FROM_PROP, E_FROM_SCORE, E_FROM_TRIMMER, K_FIN, K_NEGATIVE, K_NEGATIVE_TEXT, K_NOTIFICATION, K_POSITIVE, K_REVIEW, Q_NOTIFICATION, Q_TO_PROP, Q_TRIMMER_SCORE_FILTER


class ScoreFilter(Node):
    """
    Clase del nodo ScoreFilter.
    """
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], container_name):
        """
        Inicializa el nodo ScoreFilter.
        Declara colas y exchanges necesarios.
        """
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, container_name, n_next_nodes=n_next_nodes)
        
        # Configura las colas y los intercambios específicos para ScoreFilter
        self._middleware.declare_queue(Q_TRIMMER_SCORE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_FROM_TRIMMER, K_REVIEW)
        self._middleware.declare_exchange(E_FROM_SCORE)

        self._middleware.declare_queue(Q_TO_PROP)
        self.notification_queue = Q_NOTIFICATION + f'_{container_name}_{id}'
        self._middleware.declare_queue(self.notification_queue)
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(self.notification_queue, E_FROM_PROP, key=K_NOTIFICATION+f'_{container_name}')
        fin_key = K_FIN+f'.{container_name}'
        logging.info(f'Bindeo cola {Q_TRIMMER_SCORE_FILTER} a {E_FROM_PROP} con key {fin_key}')
        self._middleware.bind_queue(Q_TRIMMER_SCORE_FILTER, E_FROM_PROP, key=fin_key)

    def get_type(self):
        """
        Devuelve el tipo de nodo correspondiente al ScoreFilter.
        """
        return NodeType.SCORE
    
    def get_keys(self):
        """
        Obtiene un listado de keys de los siguientes nodos.
        """
        keys = []
        for node, n_nodes in self.n_next_nodes:
            if node == Q3_JOINER_CONTAINER_NAME:
                keys.append((K_POSITIVE, n_nodes))
            elif node == Q4_JOINER_CONTAINER_NAME:
                keys.append((K_NEGATIVE_TEXT, n_nodes))
            elif node == Q5_JOINER_CONTAINER_NAME:
                keys.append((K_NEGATIVE, n_nodes))
        return keys
    
    def run(self):
        """
        Inicia la recepción de mensajes de la cola principal.
        """
        while not self.shutting_down:
            try:
                logging.info("Empiezo a consumir de la cola de DATA")
                self._middleware.receive_from_queue(Q_TRIMMER_SCORE_FILTER, self._process_message, auto_ack=False)
                # Empieza a escuchar por la cola de notificaciones
                self._middleware.receive_from_queue(self.notification_queue, self._process_notification, auto_ack=False)

            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                    self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """
        Callback para procesar mensajes de la cola.
        """
        msg = decode_msg(raw_message)

        
        
        if msg.type == MsgType.REVIEWS:
            self._process_reviews_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(ch, method, msg.client_id)
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        

    def _process_reviews_message(self, msg):
        """
        Procesa mensajes de tipo REVIEWS y distribuye según el score.
        """
        negative_textreviews, positive_reviews, negative_reviews = [], [], []

        for review in msg.items:
            if review.score == Score.POSITIVE:
                positive_reviews.append(BasicReview(review.app_id))
            else:
                negative_textreviews.append(TextReview(review.app_id, review.text))
                negative_reviews.append(BasicReview(review.app_id))

        if positive_reviews:
            reviews_msg = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.BASICREVIEW, items=positive_reviews, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_POSITIVE)

        if negative_textreviews:
            reviews_msg = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.TEXTREVIEW, items=negative_textreviews, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE_TEXT)

        if negative_reviews:
            reviews_msg = ListMessage(type=MsgType.REVIEWS, item_type=ReviewsType.BASICREVIEW, items=negative_reviews, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_SCORE, reviews_msg.encode(), K_NEGATIVE)
