from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
import logging

Q_ENGLISH_JOINER_Q4 = 'english-joiner_q4'
Q_GENRE_JOINER_Q4 = 'genre-joiner_q4'
E_FROM_GENRE = 'from_genre'
K_SHOOTER_GAMES = 'shooter'

class JoinerQ4:
    def __init__(self):

        self.logger = logging.getLogger(__name__)
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_ENGLISH_JOINER_Q4)
        self._middleware.declare_queue(Q_GENRE_JOINER_Q4)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_JOINER_Q4, E_FROM_GENRE, K_SHOOTER_GAMES)

    def run(self):

        while True:
            # self.logger.custom(f'action: listening_queue: {Q_GENRE_JOINER_Q4} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_GENRE_JOINER_Q4)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_GENRE_JOINER_Q4} | result: success | msg: {msg}')
            if msg.type == MsgType.FIN:
                break
        
        while True:
            # self.logger.custom(f'action: listening_queue: {Q_ENGLISH_JOINER_Q4} | result: in_progress')
            raw_message = self._middleware.receive_from_queue(Q_ENGLISH_JOINER_Q4)
            msg = decode_msg(raw_message[2:])
            self.logger.custom(f'action: listening_queue: {Q_ENGLISH_JOINER_Q4} | result: success | msg: {msg}')
            if msg.type == MsgType.FIN:
                break