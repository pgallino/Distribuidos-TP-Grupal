from messages.messages import MsgType, Q2Games, decode_msg, Games
from middleware.middleware import Middleware
import logging

Q_GENRE_RELEASE_DATE = "genre-release_date"
E_FROM_GENRE = 'from_genre'
K_INDIE_Q2GAMES = 'indieq2'
Q_2010_GAMES = '2010_games'

class ReleaseDateFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES)
        self._middleware.declare_queue(Q_2010_GAMES)

    def run(self):
        
        # # self.logger.custom("action: listen_to_queue")

        while True:
            # # self.logger.custom('action: listening_queue | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE)
            msg = decode_msg(raw_message)
            # # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
            batch= []
            
            if msg.type == MsgType.GAMES:
                for game in msg.games:
                    if "201" in game.release_date:
                        batch.append(game)
                msg = Q2Games(msg.id, batch)
                self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                # # self.logger.custom(f"action: sending_data | result: success | data sent to {Q_2010_GAMES}")
            
            if msg.type == MsgType.FIN:
                # Se reenvia el FIN al resto de los nodos
                # # self.logger.custom("action: shutting_down | result: in_progress")
                self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                self._middleware.connection.close()
                # # self.logger.custom("action: shutting_down | result: success")
                return