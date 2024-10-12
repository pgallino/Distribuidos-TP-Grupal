from typing import List, Tuple
from node.node import Node  # Importa la clase base Node
from messages.messages import AppIDs, MsgType, decode_msg
from utils.constants import E_FROM_SCORE, Q_APPID_COUNT_ENGLISH, Q_GENRE_APPID_COUNT, E_FROM_GENRE, K_APPID_COUNT_GENRE, K_APPID_COUNT_SCORE, Q_SCORE_APPID_COUNT

class AppIdsCounter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], threshold: int = 5000):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes)
        
        # Umbral de reseñas negativas requerido para el reporte
        self.threshold = threshold

        self.app_ids_set = set()
        # Diccionario para llevar la cuenta de reseñas negativas por app_id
        self.app_ids_counts = {}

        # Configura la cola de entrada específica para AppIdsCounter
        self._middleware.declare_queue(Q_GENRE_APPID_COUNT)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_APPID_COUNT, E_FROM_GENRE, K_APPID_COUNT_GENRE)

        self._middleware.declare_queue(Q_SCORE_APPID_COUNT)
        self._middleware.declare_exchange(E_FROM_SCORE)
        self._middleware.bind_queue(Q_SCORE_APPID_COUNT, E_FROM_SCORE, K_APPID_COUNT_SCORE)

        self._middleware.declare_queue(Q_APPID_COUNT_ENGLISH)

    def run(self):
        """Inicia la recepción de mensajes de la cola Q_GENRE_APPID_COUNT."""
        try:
            self._middleware.receive_from_queue(Q_GENRE_APPID_COUNT, self._process_appids_message, auto_ack=False)
            self._middleware.receive_from_queue(Q_SCORE_APPID_COUNT, self._process_score_message, auto_ack=False)
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()

    def _process_appids_message(self, ch, method, properties, raw_message):
        """Procesa los mensajes de app_id recibidos y cuenta reseñas negativas."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.APPSIDS:
            # # self.logger.custom(f"RECIBI UN APPSIDS con: {msg.app_ids}")
            try:
                self.app_ids_set.update(msg.app_ids)
            except Exception as e:
                if not self.shutting_down:
                    self.logger.error(f"action: error update | result: fail | error: {e}")
        
        if msg.type == MsgType.FIN:

            self._middleware.channel.stop_consuming()
            # self.logger.custom("DEJE DE CONSUMIR APPSIDS")
                
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def _process_score_message(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)

        if msg.type == MsgType.REVIEWS:
            # self.logger.custom("LLEGÓ MENSAJE SCORE")
            # Incrementa el contador solo para app_ids presentes en el conjunto inicial
            for review in msg.reviews:
                if review.app_id in self.app_ids_set:
                    self.app_ids_counts[review.app_id] = self.app_ids_counts.get(review.app_id, 0) + 1
        
        elif msg.type == MsgType.FIN:
            # self.logger.custom("LLEGÓ MENSAJE FIN SCORE")
            # self.logger.custom(f"El recuento es: {self.app_ids_counts}")
            self._send_app_ids_to_english()
            self._send_fin_to_english(msg)

        ch.basic_ack(delivery_tag=method.delivery_tag)


    def _send_app_ids_to_english(self):
        """Envía los app_id que alcanzaron el umbral al nodo inglés."""
        app_ids_to_send = {app_id for app_id, count in self.app_ids_counts.items() if count >= self.threshold}
        
        if app_ids_to_send:
            app_ids_msg = AppIDs(id=self.id, app_ids=app_ids_to_send)
            encoded_msg = app_ids_msg.encode()
            for _, n_nodes in self.n_next_nodes:
                for _ in range(n_nodes):
                    self._middleware.send_to_queue(Q_APPID_COUNT_ENGLISH, encoded_msg)

        # self.logger.custom(f"action: send_app_ids_to_english | result: sent | app_ids: {app_ids_to_send}")

    def _send_fin_to_english(self, msg):
        """Envía un mensaje FIN al nodo inglés para indicar el fin del procesamiento."""

        encoded_msg = msg.encode()
        for _, n_nodes in self.n_next_nodes:
            for _ in range(n_nodes):
                self._middleware.send_to_queue(Q_APPID_COUNT_ENGLISH, encoded_msg)
        
        self.logger.info("action: send_fin_to_english | result: sent")
        