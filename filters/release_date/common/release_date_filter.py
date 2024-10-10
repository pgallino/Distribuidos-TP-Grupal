from messages.messages import MsgType, Q2Games, decode_msg
import signal
from middleware.middleware import Middleware
import logging

Q_GENRE_RELEASE_DATE = "genre-release_date"
E_FROM_GENRE = 'from_genre'
K_INDIE_Q2GAMES = 'indieq2'
Q_2010_GAMES = '2010_games'

class ReleaseDateFilter:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES)
        self._middleware.declare_queue(Q_2010_GAMES)

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._middleware.connection.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)
            batch = []

            if msg.type == MsgType.GAMES:
                for game in msg.games:
                    if "201" in game.release_date:
                        batch.append(game)

                # Crear y enviar el mensaje `Q2Games` si hay juegos en el batch
                if batch:
                    games_msg = Q2Games(msg.id, batch)
                    self._middleware.send_to_queue(Q_2010_GAMES, games_msg.encode())
                    
            elif msg.type == MsgType.FIN:
                # Reenvía el mensaje FIN y cierra la conexión
                self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                self.shutting_down = True
                self._middleware.connection.close()

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE, process_message)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
