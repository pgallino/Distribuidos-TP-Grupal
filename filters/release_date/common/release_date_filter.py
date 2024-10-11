from messages.messages import MsgType, decode_msg
from messages.games_msg import Q2Games
import signal
from middleware.middleware import Middleware
import logging

Q_GENRE_RELEASE_DATE = "genre-release_date"
E_FROM_GENRE = 'from_genre'
E_COORD_RELEASE_DATE = 'from-coord-release_date'
K_INDIE_Q2GAMES = 'indieq2'
Q_2010_GAMES = '2010_games'
Q_COORD_RELEASE_DATE = 'coord-release_date'

class ReleaseDateFilter:

    def __init__(self, id: int, n_nodes: int):

        self.id = id
        self.n_nodes = n_nodes

        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GENRE_RELEASE_DATE)
        self._middleware.declare_exchange(E_FROM_GENRE)
        self._middleware.bind_queue(Q_GENRE_RELEASE_DATE, E_FROM_GENRE, K_INDIE_Q2GAMES)
        self._middleware.declare_queue(Q_2010_GAMES)

        if self.n_nodes > 1:
            self.coordination_queue = Q_COORD_RELEASE_DATE + f"{self.id}"
            self._middleware.declare_queue(self.coordination_queue)
            self._middleware.declare_exchange(E_COORD_RELEASE_DATE)
            for i in range(1, self.n_nodes + 1):
                if i != self.id:
                    routing_key = f"coordination_{i}"
                    self._middleware.bind_queue(self.coordination_queue, E_COORD_RELEASE_DATE, routing_key)
            self.fins_counter = 1

    def _shutdown(self):
        if self.shutting_down:
            return
        self.shutting_down = True
        self._server_socket.close()
        self._middleware.channel.stop_consuming()
        self._middleware.channel.close()
        self._middleware.connection.close()

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            self.fins_counter += 1
            if self.fins_counter == self.n_nodes:
                if self.id == 1:
                # Reenvía el mensaje FIN y cierra la conexión
                    self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._shutdown()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

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
                self._middleware.channel.stop_consuming()

                if self.n_nodes > 1:
                    self._middleware.send_to_queue(E_COORD_RELEASE_DATE, msg.encode(), key=f"coordination_{self.id}")
                else:
                    self.logger.custom(f"Soy el nodo lider {self.id}, mando los FINs")
                    self._middleware.send_to_queue(Q_2010_GAMES, msg.encode())

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_GENRE_RELEASE_DATE, process_message)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)

        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()
