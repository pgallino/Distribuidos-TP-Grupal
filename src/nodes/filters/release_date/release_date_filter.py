import logging
from typing import List, Tuple
from messages.games_msg import GamesType
from messages.messages import ListMessage, MsgType, NodeType, decode_msg
from node import Node  # Importa la clase base Node
from utils.constants import E_COORD_RELEASE_DATE, E_FROM_GENRE, E_FROM_PROP, K_FIN, K_INDIE_Q2GAMES, K_NOTIFICATION, Q_NOTIFICATION, Q_RELEASE_DATE_AVG_COUNTER, Q_COORD_RELEASE_DATE, Q_GENRE_RELEASE_DATE, Q_TO_PROP

class ReleaseDateFilter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], container_name):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, container_name, n_next_nodes=n_next_nodes)
        
        # Configura las colas y los intercambios específicos para ReleaseDateFilter
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES)
        self._middleware.declare_queue(Q_RELEASE_DATE_AVG_COUNTER)

        self._middleware.declare_queue(Q_TO_PROP)
        self.notification_queue = Q_NOTIFICATION + f'_{container_name}_{id}'
        self._middleware.declare_queue(self.notification_queue)
        self._middleware.declare_exchange(E_FROM_PROP)
        self._middleware.bind_queue(self.notification_queue, E_FROM_PROP, key=K_NOTIFICATION+f'_{container_name}')
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_PROP, key=K_FIN+f'_{container_name}')

    def get_type(self):
        return NodeType.RELEASE_DATE
    
    def get_keys(self):
        return [('', 1)]

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        while not self.shutting_down:
            try:
                logging.info("Empiezo a consumir de la cola de DATA")
                self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE, self._process_message, auto_ack=False)
                # Empieza a escuchar por la cola de notificaciones
                self._middleware.receive_from_queue(self.notification_queue, self._process_notification, auto_ack=False)

            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                    self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_GENRE_RELEASE_DATE."""
        msg = decode_msg(raw_message)

        
        
        if msg.type == MsgType.GAMES:
            self._process_games_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(ch, method, msg.client_id)
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        

    def _process_games_message(self, msg):
        """Filtra y envía juegos lanzados en 2010 o después."""
        batch = [game for game in msg.items if "201" in game.release_date]
        if batch:
            games_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.Q2GAMES, items=batch, client_id=msg.client_id)
            self._middleware.send_to_queue(Q_RELEASE_DATE_AVG_COUNTER, games_msg.encode())

