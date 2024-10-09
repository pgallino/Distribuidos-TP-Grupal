from messages.messages import MsgType, decode_msg, Result, QueryNumber
from middleware.middleware import Middleware
import logging
import heapq

Q_2010_GAMES = '2010_games'
Q_QUERY_RESULT_2 = "query_result_2"

AVG_PLAYTIME = 0

class AvgCounter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_2010_GAMES)
        self._middleware.declare_queue(Q_QUERY_RESULT_2)

        # Estructuras para almacenar datos
        self.top_10_games = []  # Almacenará el top 10 de juegos de la década del 2010 con mayor average playtime

    def run(self):

        # self.logger.custom("action: listen_to_queue")

        while True:
            # self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_2010_GAMES)
            msg = decode_msg(raw_message)
            # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            
            if msg.type == MsgType.GAMES:
                for game in msg.games:
                    if len(self.top_10_games) < 10:
                        heapq.heappush(self.top_10_games, (game.avg_playtime, game.app_id, game))
                    elif game.avg_playtime > self.top_10_games[0][AVG_PLAYTIME]:
                        heapq.heapreplace(self.top_10_games, (game.avg_playtime, game.app_id, game))
            
            if msg.type == MsgType.FIN:

                # Obtener el top 10 de juegos de la década del 2010 con mayor average playtime
                result = sorted(self.top_10_games, key=lambda x: x[AVG_PLAYTIME], reverse=True)

                # Crear un mensaje concatenado con los resultados
                top_message = f"Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
                for avg_playtime, app_id, game in result:
                    top_message += f"- {game.name}: {avg_playtime} average playtime\n"

                # Loggear el mensaje completo
                # self.logger.custom(top_message)

                # Crear el mensaje Result
                result_message = Result(id=msg.id, query_number=QueryNumber.Q2.value, result=top_message)

                # Enviar el mensaje Result al exchange de resultados
                self._middleware.send_to_queue(Q_QUERY_RESULT_2, result_message.encode())

                # Cierre de la conexión
                # self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.connection.close()
                # self.logger.custom("action: shutting_down | result: success")

                return