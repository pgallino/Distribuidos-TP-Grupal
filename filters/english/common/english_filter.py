import signal
from messages.messages import BasicReview, BasicReviews, MsgType, decode_msg, Reviews
from middleware.middleware import Middleware
import logging
import langid

Q_SCORE_ENGLISH = "score_filter-english_filter"
E_FROM_SCORE = 'from_score'
E_COORD_ENGLISH = 'from-coord-english'
K_NEGATIVE_TEXT = 'negative_text'
Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_COORD_ENGLISH = 'coord-english'

class EnglishFilter:

    def __init__(self, id: int, n_nodes: int):

        self.id = id
        self.n_nodes = n_nodes
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_Q4_JOINER)
        self._middleware.declare_queue(Q_SCORE_ENGLISH)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_ENGLISH, E_FROM_SCORE, K_NEGATIVE_TEXT)

        
        if self.n_nodes > 1:
            self.coordination_queue = Q_COORD_ENGLISH + f"{self.id}"
            self._middleware.declare_queue(self.coordination_queue)
            self._middleware.declare_exchange(E_COORD_ENGLISH)
            for i in range(1, self.n_nodes + 1):
                if i != self.id:
                    routing_key = f"coordination_{i}"
                    self._middleware.bind_queue(self.coordination_queue, E_COORD_ENGLISH, routing_key)
            self.fins_counter = 1


        # cuando me llega el fin, dejo de escuchar la cola original
        # envio el fin a la cola Q_COORDINATOR con la KEY de mi id (la unica key que no escucho)
        # me quedo escuchando mi cola Q_COORDINATOR con la KEY del resto de los nodos
        # cuento los fin que recibo (n_nodes - 1)
        # si soy el nodo de id 1 envio la cantidad de Fins necesaria al siguiente nodo
        # Q_COORD_ENGLISH
    
    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def is_english(self, text):
        # Detectar idioma usando langid
        lang, _ = langid.classify(text)
        # self.logger.custom(f"is english: {lang}")
        return lang == 'en'  # Retorna True si el idioma detectado es inglés


    def process_fin(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            self.fins_counter += 1
            if self.id == 1 and self.fins_counter == self.n_nodes:
                # Reenvía el mensaje FIN y cierra la conexión
                self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.shutting_down = True
                self._middleware.connection.close()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            en_reviews = []

            msg = decode_msg(raw_message)

            if msg.type == MsgType.REVIEWS:
                # Filtrar reseñas en inglés
                for review in msg.reviews:
                    if self.is_english(review.text):
                        basic_review = BasicReview(review.app_id)
                        en_reviews.append(basic_review)
                
                # Enviar el batch de reseñas en inglés si contiene elementos
                if en_reviews:
                    english_reviews_msg = BasicReviews(id=msg.id, reviews=en_reviews)
                    self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, english_reviews_msg.encode())

            elif msg.type == MsgType.FIN:
                
                # Reenvía el mensaje FIN y cierra la conexión
                self._middleware.channel.stop_consuming()

                if self.n_nodes > 1:
                    self._middleware.send_to_queue(E_COORD_ENGLISH, msg.encode(), key=f"coordination_{self.id}")
                else:
                    self.logger.custom(f"Solo soy UN nodo {self.id}, Propago el Fin normalmente")
                    self._middleware.send_to_queue(Q_ENGLISH_Q4_JOINER, msg.encode())

            ch.basic_ack(delivery_tag=method.delivery_tag)

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_SCORE_ENGLISH, process_message, auto_ack=False)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)
            else:
                self.shutting_down = True
                self._middleware.connection.close()
                
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")