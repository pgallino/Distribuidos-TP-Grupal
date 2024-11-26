import logging
from typing import List, Tuple
from messages.messages import ListMessage, MsgType, decode_msg
from messages.games_msg import GamesType, Q2Game, BasicGame, Genre
from node import Node  # Importa la clase base Node
from utils.constants import E_COORD_GENRE, E_FROM_GENRE, E_FROM_TRIMMER, K_GENREGAME, K_INDIE_BASICGAMES, K_INDIE_Q2GAMES, K_SHOOTER_GAMES, Q_COORD_GENRE, Q_TRIMMER_GENRE_FILTER


class GenreFilter(Node):
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        # Inicializa la clase base Node
        super().__init__(id, n_nodes, n_next_nodes)

        # Configura las colas y los intercambios específicos para GenreFilter
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GENREGAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

        if self.n_nodes > 1: self._middleware.declare_exchange(E_COORD_GENRE)


    def get_keys(self):
        keys = []
        for node, n_nodes in self.n_next_nodes:
            if node == 'RELEASE_DATE':
                keys.append((K_INDIE_Q2GAMES, n_nodes))
            elif node == 'JOINER_Q3':
                keys.append((K_INDIE_BASICGAMES, n_nodes))
            elif node == 'JOINER_Q4':
                keys.append((K_SHOOTER_GAMES, n_nodes))
        return keys
    
    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:
            if self.n_nodes > 1:
                self.init_coordinator(self.id, Q_COORD_GENRE, E_COORD_GENRE, self.n_nodes, self.get_keys(), E_FROM_GENRE)
            self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER, self._process_message, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_TRIMMER_GENRE_FILTER."""
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
        """Filtra juegos por género y los envía a las colas correspondientes."""
        indie_basic_games, indie_q2_games, shooter_games = [], [], []

        for game in msg.items:
            for genre in game.genres:
                if genre == Genre.INDIE.value:
                    indie_basic_games.append(BasicGame(app_id=game.app_id, name=game.name))
                    indie_q2_games.append(Q2Game(app_id=game.app_id, name=game.name, release_date=game.release_date, avg_playtime=game.avg_playtime))
                elif genre == Genre.ACTION.value:
                    shooter_games.append(BasicGame(app_id=game.app_id, name=game.name))

        if indie_basic_games:
            indie_basic_msg = ListMessage(type=MsgType.GAMES, msg_id=0, item_type=GamesType.BASICGAME, items=indie_basic_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, indie_basic_msg.encode(), key=K_INDIE_BASICGAMES)

        if indie_q2_games:
            indie_q2_msg = ListMessage(type=MsgType.GAMES, msg_id=0, item_type=GamesType.Q2GAMES, items=indie_q2_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, indie_q2_msg.encode(), key=K_INDIE_Q2GAMES)
        
        if shooter_games:
            shooter_msg = ListMessage(type=MsgType.GAMES, msg_id=0, item_type=GamesType.BASICGAME, items=shooter_games, client_id=msg.client_id)
            self._middleware.send_to_queue(E_FROM_GENRE, shooter_msg.encode(), key=K_SHOOTER_GAMES)

    def _process_fin_message(self, msg):
        """Reenvía el mensaje FIN y cierra la conexión si es necesario."""
        if self.n_nodes > 1:
            self.forward_coordfin(E_COORD_GENRE, msg)
        else:
            for node, _ in self.n_next_nodes:
                if node == 'RELEASE_DATE':
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_Q2GAMES)
                elif node == 'JOINER_Q3':
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_BASICGAMES)
                elif node == 'JOINER_Q4':
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)
                # TODO
                # VER QUE HACER CON JOINER Q5
