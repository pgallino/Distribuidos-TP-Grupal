import signal
from messages.messages import Genre, MsgType, decode_msg, Games
from middleware.middleware import Middleware
import logging

Q_TRIMMER_GENRE_FILTER = "trimmer-genre_filter"
E_FROM_TRIMMER = 'trimmer-filters'
K_GAME = 'game'
E_FROM_GENRE = 'from_genre'
K_INDIE_GAMES = 'indie'
K_SHOOTER_GAMES = 'shooter'

class GenreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        try:
            # self.logger.custom("action: listen_to_queue")
            while True:
                # self.logger.custom('action: listening_queue | result: in_progress')
                raw_message = self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER)
                msg = decode_msg(raw_message)
                # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
                if msg.type == MsgType.GAMES:
                    # Procesar cada juego en el mensaje `Games`
                    indie_games = []
                    shooter_games = []
                    
                    for game in msg.games:
                        # self.logger.custom(f"GENEROS: {game.genres}")
                        for genre in game.genres:
                            if genre == Genre.INDIE:
                                indie_games.append(game)
                            if genre == Genre.ACTION:
                                shooter_games.append(game)

                    # Enviar mensajes `Games` con los juegos filtrados
                    if indie_games:
                        # self.logger.custom(f"Indies: {indie_games}")
                        indie_msg = Games(id=msg.id, games=indie_games)
                        self._middleware.send_to_queue(E_FROM_GENRE, indie_msg.encode(), key=K_INDIE_GAMES)
                    
                    if shooter_games:
                        shooter_msg = Games(id=msg.id, games=shooter_games)
                        self._middleware.send_to_queue(E_FROM_GENRE, shooter_msg.encode(), key=K_SHOOTER_GAMES)
                elif msg.type == MsgType.FIN:
                    # Se reenvia el FIN al resto de los nodos
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_GAMES)
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return

        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")