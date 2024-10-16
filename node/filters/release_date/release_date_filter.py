from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.games_msg import Q2Games
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

        # Configura la cola de coordinación
        self._setup_coordination_queue(Q_COORD_RELEASE_DATE, E_COORD_RELEASE_DATE)

    def run(self):
        """Inicia la recepción de mensajes de la cola."""
        try:
            self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE, self._process_message, auto_ack=False)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

        finally:
            self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        """Procesa mensajes de tipo FIN y coordina con otros nodos."""
        self._process_fin(raw_message, [('', 1)], Q_RELEASE_DATE_AVG_COUNTER)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar mensajes de la cola Q_GENRE_RELEASE_DATE."""
        msg = decode_msg(raw_message)
        
        if msg.type == MsgType.GAMES:
            self._process_games_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_games_message(self, msg):
        """Filtra y envía juegos lanzados en 2010 o después."""
        batch = [game for game in msg.games if "201" in game.release_date]
        if batch:
            games_msg = Q2Games(msg.id, batch)
            self._middleware.send_to_queue(Q_RELEASE_DATE_AVG_COUNTER, games_msg.encode())

    def _process_fin_message(self, msg):
        """Reenvía el mensaje FIN y cierra la conexión si es necesario."""
        self._middleware.channel.stop_consuming()
        
        if self.n_nodes > 1:
            self._middleware.send_to_queue(E_COORD_RELEASE_DATE, msg.encode(), key=f"coordination_{self.id}")
        else:
            self._middleware.send_to_queue(Q_RELEASE_DATE_AVG_COUNTER, msg.encode())

