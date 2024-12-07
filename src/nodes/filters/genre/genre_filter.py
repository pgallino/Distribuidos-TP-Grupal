import logging
from typing import List, Tuple
from messages.messages import ListMessage, MsgType, decode_msg
from messages.games_msg import GamesType, Q2Game, BasicGame, Genre
from node import Node  # Importa la clase base Node
from utils.container_constants import Q3_JOINER_CONTAINER_NAME, Q4_JOINER_CONTAINER_NAME, RELEASE_DATE_CONTAINER_NAME
from utils.utils import NodeType
from utils.middleware_constants import E_FROM_GENRE, E_FROM_PROP, E_FROM_TRIMMER, K_FIN, K_GENREGAME, K_INDIE_BASICGAMES, K_INDIE_Q2GAMES, K_NOTIFICATION, K_SHOOTER_GAMES, Q_NOTIFICATION, Q_TO_PROP, Q_TRIMMER_GENRE_FILTER

class GenreFilter(Node):
    """
    Clase del nodo GenreFilter.
    """
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]], container_name):
        """
        Inicializa el nodo GenreFilter.
        Declara colas y exchanges necesarios.
        """
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, container_name, n_next_nodes=n_next_nodes)

        # Configura las colas y los intercambios específicos para GenreFilter
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GENREGAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

        self._middleware.declare_queue(Q_TO_PROP)
        self.notification_queue = Q_NOTIFICATION + f'_{container_name}_{id}'
        self._middleware.declare_queue(self.notification_queue)
        self._middleware.declare_exchange(E_FROM_PROP, type='topic')
        self._middleware.bind_queue(self.notification_queue, E_FROM_PROP, key=K_NOTIFICATION+f'_{container_name}')
        fin_key = K_FIN+f'.{container_name}'
        # logging.info(f'Bindeo cola {Q_TRIMMER_GENRE_FILTER} a {E_FROM_PROP} con key {fin_key}')
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_PROP, key=fin_key)

    def get_type(self):
        """
        Devuelve el tipo de nodo correspondiente al GenreFilter.
        """
        return NodeType.GENRE

    def get_keys(self):
        """
        Obtiene un listado de keys de los siguientes nodos.
        """
        keys = []
        for node, n_nodes in self.n_next_nodes:
            if node == RELEASE_DATE_CONTAINER_NAME:
                keys.append((K_INDIE_Q2GAMES, n_nodes))
            elif node == Q3_JOINER_CONTAINER_NAME:
                keys.append((K_INDIE_BASICGAMES, n_nodes))
            elif node == Q4_JOINER_CONTAINER_NAME:
                keys.append((K_SHOOTER_GAMES, n_nodes))
        return keys
    
    def run(self):
        """
        Inicia la recepción de mensajes de la cola principal.
        """
        while not self.shutting_down:
            try:
                #logging.info("Empiezo a consumir de la cola de DATA")
                self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER, self._process_message, auto_ack=False)
                # Empieza a escuchar por la cola de notificaciones
                self._middleware.receive_from_queue(self.notification_queue, self._process_notification, auto_ack=False)

            except Exception as e:
                if not self.shutting_down:
                    logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                    self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """
        Callback para procesar mensajes de la cola Q_TRIMMER_GENRE_FILTER.
        """
        msg = decode_msg(raw_message)
        if msg.type == MsgType.GAMES:
            self._process_games_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(ch, method, msg.client_id)
            return
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_games_message(self, msg):
        """
        Filtra juegos por género y los envía a las colas correspondientes.
        """
        indie_basic_games, indie_q2_games, shooter_games = [], [], []

        for game in msg.items:
            for genre in game.genres:
                if genre == Genre.INDIE.value:
                    indie_basic_games.append(BasicGame(app_id=game.app_id, name=game.name))
                    indie_q2_games.append(Q2Game(app_id=game.app_id, name=game.name, release_date=game.release_date, avg_playtime=game.avg_playtime))
                elif genre == Genre.ACTION.value:
                    shooter_games.append(BasicGame(app_id=game.app_id, name=game.name))

        if indie_basic_games:
            indie_basic_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.BASICGAME, items=indie_basic_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, indie_basic_msg.encode(), key=K_INDIE_BASICGAMES)

        if indie_q2_games:
            indie_q2_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.Q2GAMES, items=indie_q2_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, indie_q2_msg.encode(), key=K_INDIE_Q2GAMES)
        
        if shooter_games:
            shooter_msg = ListMessage(type=MsgType.GAMES, item_type=GamesType.BASICGAME, items=shooter_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, shooter_msg.encode(), key=K_SHOOTER_GAMES)
