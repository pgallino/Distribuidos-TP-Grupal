import signal
import traceback
from messages.messages import MsgType, decode_msg
from messages.results_msg import Q2Result
from middleware.middleware import Middleware
import logging
import heapq

from utils.constants import Q_2010_GAMES, Q_QUERY_RESULT_2

AVG_PLAYTIME = 0

class AvgCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_2010_GAMES)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)

        # Estructuras para almacenar datos
        self.top_10_games = []  # Almacenará el top 10 de juegos de la década del 2010 con mayor average playtime

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.channel.stop_consuming()
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)

            if msg.type == MsgType.GAMES:
                for game in msg.games:
                    if len(self.top_10_games) < 10:
                        heapq.heappush(self.top_10_games, (game.avg_playtime, game.app_id, game))
                    elif game.avg_playtime > self.top_10_games[0][0]:  # 0 es el índice de avg_playtime
                        heapq.heapreplace(self.top_10_games, (game.avg_playtime, game.app_id, game))

            elif msg.type == MsgType.FIN:
                # Ordenar y obtener el top 10 de juegos por average playtime
                result = sorted(self.top_10_games, key=lambda x: x[0], reverse=True)
                top_games = [(game.name, avg_playtime) for avg_playtime, app_id, game in result]

                # Crear y enviar el mensaje de resultado
                result_message = Q2Result(id=msg.id, top_games=top_games)
                self._middleware.send_to_queue(Q_QUERY_RESULT_2, result_message.encode())
                
                # Marcar el cierre en proceso y cerrar la conexión
                self.shutting_down = True
                self._middleware.connection.close()

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_2010_GAMES, process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                traceback.print_exc()
