from typing import List
from messages.messages import MsgType, decode_msg
from messages.reviews_msg import BasicReview, BasicReviews
from node.node import Node  # Importa la clase base Node
from utils.constants import E_COORD_ENGLISH, E_FROM_SCORE, K_NEGATIVE_TEXT, Q_COORD_ENGLISH, Q_ENGLISH_Q4_JOINER, Q_SCORE_ENGLISH, Q_APPID_COUNT_ENGLISH
import langid


class EnglishFilter(Node):
    def __init__(self, id: int, n_nodes: int):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes=[])

        # Configura las colas y los intercambios específicos para EnglishFilter
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_APPID_COUNT_ENGLISH)
        self._middleware.declare_queue(Q_SCORE_ENGLISH)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_ENGLISH, E_FROM_SCORE, K_NEGATIVE_TEXT)

        # Configura la cola de coordinación
        self._setup_coordination_queue(Q_COORD_ENGLISH, E_COORD_ENGLISH)

        # Conjunto para almacenar los app_ids recibidos del contador
        self.app_ids_set = set()

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:

            # Primero escucha la cola de app_ids del contador
            self._middleware.receive_from_queue(Q_APPID_COUNT_ENGLISH, self._process_app_ids_message, auto_ack=False)

            # Luego, escucha las reseñas
            self._middleware.receive_from_queue(Q_SCORE_ENGLISH, self._process_reviews_message, auto_ack=False)

            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self._process_fin, auto_ack=False)
            else:
                self._shutdown()

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()

    def _process_fin(self, ch, method, properties, raw_message):
        """Procesa mensajes de tipo FIN y coordina con otros nodos."""
        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            self.fins_counter += 1
            if self.fins_counter == self.n_nodes:
                if self.id == 1:
                    self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._shutdown()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_english(self, msg):
        """Filtra y envía reseñas en inglés a la cola correspondiente."""

        # # self.logger.custom("RECIBI REVIEWS DE SCORE")
        en_reviews = [
            BasicReview(review.app_id) for review in msg.reviews if self.is_english(review.text)
        ]

        if en_reviews:
            english_reviews_msg = BasicReviews(id=msg.id, reviews=en_reviews)
            # # self.logger.custom(f"ESTAS SON LAS REVIEWS EN INGLES {english_reviews_msg}")
            self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, english_reviews_msg.encode())
            # self.logger.custom(f"ENVIE A Q4JOINER {english_reviews_msg}")

    def _process_fin_message(self, msg):
        """Reenvía el mensaje FIN y cierra la conexión si es necesario."""
        self._middleware.channel.stop_consuming()
        
        if self.n_nodes > 1:
            self._middleware.send_to_queue(E_COORD_ENGLISH, msg.encode(), key=f"coordination_{self.id}")
        else:
            self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())

    def is_english(self, text):
        """Detecta si el texto está en inglés usando langid."""
        lang, _ = langid.classify(text)
        return lang == 'en'  # Retorna True si el idioma detectado es inglés

    def _process_reviews_message(self, ch, method, properties, raw_message):
        """Filtra y envía reseñas en inglés a la cola correspondiente, solo si el app_id está en el conjunto."""

        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            # self.logger.custom("RECIBI REVIEWS")
            self._process_english(msg)
        
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _process_app_ids_message(self, ch, method, properties, raw_message):
        """Procesa los mensajes de app_ids recibidos desde el contador."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.APPSIDS:
            # # self.logger.custom(f"recibi mensaje appid con {msg.app_ids}")
            self.app_ids_set.update(msg.app_ids)  # Agrega los app_ids al conjunto
            self.logger.info(f"action: process_app_ids | app_ids: {msg.app_ids}")

        elif msg.type == MsgType.FIN:
            # # self.logger.custom("RECIBI FIN DE APPID")
            self._middleware.channel.stop_consuming()

        ch.basic_ack(delivery_tag=method.delivery_tag)
