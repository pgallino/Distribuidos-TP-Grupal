import logging
from typing import List, Tuple
from messages.games_msg import GamesType
from messages.messages import ListMessage, MsgType, decode_msg
from node import Node  # Importa la clase base Node
from utils.constants import E_COORD_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES, Q_RELEASE_DATE_AVG_COUNTER, Q_COORD_RELEASE_DATE, Q_GENRE_RELEASE_DATE

class ReleaseDateFilter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes)
        
        # Configura las colas y los intercambios específicos para ReleaseDateFilter
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES)
        self._middleware.declare_queue(Q_RELEASE_DATE_AVG_COUNTER)

        if self.n_nodes > 1: self._middleware.declare_exchange(E_COORD_RELEASE_DATE)
    
    def get_keys(self):
        return [('', 1)]

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:
            if self.n_nodes > 1:
                self.init_coordinator(self.id, Q_COORD_RELEASE_DATE, E_COORD_RELEASE_DATE, self.n_nodes, self.get_keys(), Q_RELEASE_DATE_AVG_COUNTER)
            self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE, self._process_message, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_GENRE_RELEASE_DATE."""
        msg = decode_msg(raw_message)

        with self.condition:
            self.processing_client.value = msg.client_id # SETEO EL ID EN EL processing_client -> O SEA ESTOY PROCESANDO UN MENSAJE DE CLIENTE ID X
            self.condition.notify_all()
        
        if msg.type == MsgType.GAMES:
            self._process_games_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        with self.condition:
            self.processing_client.value = -1 # SETEO EL -1 EN EL processing_client -> TERMINE DE PROCESAR
            self.condition.notify_all()

    def _process_games_message(self, msg):
        """Filtra y envía juegos lanzados en 2010 o después."""
        batch = [game for game in msg.items if "201" in game.release_date]
        if batch:
            games_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.Q2GAMES, items=batch, client_id=msg.client_id)
            self._middleware.send_to_queue(Q_RELEASE_DATE_AVG_COUNTER, games_msg.encode())

    def _process_fin_message(self, msg):
        """Reenvía el mensaje FIN y cierra la conexión si es necesario."""
        
        if self.n_nodes > 1:
            self.forward_coordfin(E_COORD_RELEASE_DATE, msg)
        else:
            self._middleware.send_to_queue(Q_RELEASE_DATE_AVG_COUNTER, msg.encode())

