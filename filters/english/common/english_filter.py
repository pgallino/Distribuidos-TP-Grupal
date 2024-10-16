from typing import List
from messages.messages import MsgType, decode_msg
from messages.reviews_msg import BasicReview, BasicReviews
from node.node import Node  # Importa la clase base Node
from utils.constants import E_COORD_ENGLISH, Q_COORD_ENGLISH, Q_ENGLISH_Q4_JOINER, Q_Q4_JOINER_ENGLISH
import langid


class EnglishFilter(Node):
    def __init__(self, id: int, n_nodes: int):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes=[])

        # Configura las colas y los intercambios específicos para EnglishFilter
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_Q4_JOINER_ENGLISH)

        # Configura la cola de coordinación
        self._setup_coordination_queue(Q_COORD_ENGLISH, E_COORD_ENGLISH)

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:
            self._middleware.receive_from_queue(Q_Q4_JOINER_ENGLISH, self._process_message, auto_ack=False)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self._process_fin, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

        finally:
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
                self._middleware.channel.stop_consuming()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_SCORE_ENGLISH."""
        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.REVIEWS:
            self._process_reviews_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_reviews_message(self, msg):
        """Filtra y envía reseñas en inglés a la cola correspondiente."""
        en_reviews = [
            BasicReview(review.app_id) for review in msg.reviews if self.is_english(review.text)
        ]

        if en_reviews:
            english_reviews_msg = BasicReviews(id=msg.id, reviews=en_reviews)
            self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, english_reviews_msg.encode())

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
