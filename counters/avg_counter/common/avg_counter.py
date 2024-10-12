import signal
import traceback
from typing import List, Tuple
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q2Result
import heapq

from node.node import Node
from utils.constants import Q_2010_GAMES, Q_QUERY_RESULT_2

class AvgCounter(Node):

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):
        super().__init__(id, n_nodes, n_next_nodes)

        self._middleware.declare_queue(Q_2010_GAMES)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)

        # Estructuras para almacenar datos
        self.top_10_games = []  # Almacenará el top 10 de juegos de la década del 2010 con mayor average playtime

    def run(self):

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_2010_GAMES, self._process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                traceback.print_exc()

    def _process_message(self, ch, method, properties, raw_message):
        """Callback para procesar el mensaje de la cola."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.GAMES:
            self._process_game_message(msg)

        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)
    
    def _process_game_message(self, msg):
        for game in msg.games:
            if len(self.top_10_games) < 10:
                heapq.heappush(self.top_10_games, (game.avg_playtime, game.app_id, game))
            elif game.avg_playtime > self.top_10_games[0][0]:  # 0 es el índice de avg_playtime
                heapq.heapreplace(self.top_10_games, (game.avg_playtime, game.app_id, game))
    
    def _process_fin_message(self, msg):
        # Ordenar y obtener el top 10 de juegos por average playtime
        result = sorted(self.top_10_games, key=lambda x: x[0], reverse=True)
        top_games = [(game.name, avg_playtime) for avg_playtime, _, game in result]

        # Crear y enviar el mensaje de resultado
        result_message = Q2Result(id=msg.id, top_games=top_games)
        self._middleware.send_to_queue(Q_QUERY_RESULT_2, result_message.encode())
        
        # Marcar el cierre en proceso y cerrar la conexión
        self._shutdown()