from messages.messages import Genre, MsgType, Q2Game, Q2Games, decode_msg, BasicGame, BasicGames
import signal
from middleware.middleware import Middleware
import logging

Q_TRIMMER_GENRE_FILTER = "trimmer-genre_filter"
E_FROM_TRIMMER = 'trimmer-filters'
K_GENREGAME = 'genregame'
E_FROM_GENRE = 'from_genre'
K_INDIE_Q2GAMES = 'indieq2'
K_INDIE_BASICGAMES = 'indiebasic'
K_SHOOTER_GAMES = 'shooter'

class GenreFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GENREGAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        # self.logger.custom("action: listen_to_queue")

        try:

            while True:
                # self.logger.custom('action: listening_queue | result: in_progress')
                raw_message = self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER)
                msg = decode_msg(raw_message)
                # self.logger.custom(f'action: listening_queue | result: success | msg: {msg}')
                if msg.type == MsgType.GAMES:
                    # Procesar cada juego en el mensaje `Games`
                    indie_basic_games = []
                    indie_q2_games = []
                    shooter_games = []
                    
                    for game in msg.games:
                        # self.logger.custom(f"GENEROS: {game.genres}")
                        for genre in game.genres:
                            if genre == Genre.INDIE.value:
                                # Crear instancias de BasicGame y Q2Game para los juegos Indie
                                basic_game = BasicGame(app_id=game.app_id, name=game.name)
                                q2_game = Q2Game(app_id=game.app_id, name=game.name, release_date=game.release_date, avg_playtime=game.avg_playtime)

                                indie_basic_games.append(basic_game)
                                indie_q2_games.append(q2_game)

                            if genre == Genre.ACTION.value:
                                # Crear una instancia de `BasicGame` y añadirla al lote de shooters
                                basic_game = BasicGame(app_id=game.app_id, name=game.name)
                                shooter_games.append(basic_game)

                    # Enviar mensajes `Games` con los juegos Indie filtrados
                    if indie_basic_games:
                        indie_basic_msg = BasicGames(id=msg.id, games=indie_basic_games)
                        self._middleware.send_to_queue(E_FROM_GENRE, indie_basic_msg.encode(), key=K_INDIE_BASICGAMES)

                    if indie_q2_games:
                        indie_q2_msg = Q2Games(id=msg.id, games=indie_q2_games)
                        self._middleware.send_to_queue(E_FROM_GENRE, indie_q2_msg.encode(), key=K_INDIE_Q2GAMES)
                    
                    if shooter_games:
                        shooter_msg = BasicGames(id=msg.id, games=shooter_games)
                        self._middleware.send_to_queue(E_FROM_GENRE, shooter_msg.encode(), key=K_SHOOTER_GAMES)

                elif msg.type == MsgType.FIN:
                    # Se reenvia el FIN al resto de los nodos
                    # self.logger.custom("action: shutting_down | result: in_progress")
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_Q2GAMES)
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_BASICGAMES)
                    self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)
                    self._middleware.connection.close()
                    # self.logger.custom("action: shutting_down | result: success")
                    return

        except Exception as e:
            self.logger.custom(f"Esta haciendo shutting_down: {self.shutting_down}")
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
