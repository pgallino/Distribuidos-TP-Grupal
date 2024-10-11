from messages.messages import Genre, MsgType, decode_msg
from messages.games_msg import Q2Game, Q2Games, BasicGame, BasicGames
import signal
from typing import List, Tuple
from middleware.middleware import Middleware
import logging

from utils.constants import E_COORD_GENRE, E_FROM_GENRE, E_FROM_TRIMMER, K_GENREGAME, K_INDIE_BASICGAMES, K_INDIE_Q2GAMES, K_SHOOTER_GAMES, Q_COORD_GENRE, Q_TRIMMER_GENRE_FILTER

class GenreFilter:

    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):

        self.id = id
        self.n_nodes = n_nodes
        self.n_next_nodes = n_next_nodes
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

        self._middleware = Middleware()
        self._middleware.declare_queue(Q_TRIMMER_GENRE_FILTER)
        self._middleware.declare_exchange(E_FROM_TRIMMER)
        self._middleware.bind_queue(Q_TRIMMER_GENRE_FILTER, E_FROM_TRIMMER, K_GENREGAME)
        self._middleware.declare_exchange(E_FROM_GENRE)

        if self.n_nodes > 1:
            self.coordination_queue = Q_COORD_GENRE + f"{self.id}"
            self._middleware.declare_queue(self.coordination_queue)
            self._middleware.declare_exchange(E_COORD_GENRE)
            for i in range(1, self.n_nodes + 1): # arranco en el id 1 y sigo hasta el numero de nodos
                if i != self.id:
                    self._middleware.bind_queue(self.coordination_queue, E_COORD_GENRE, f"coordination_{i}")
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
        # self.logger.custom("Received SIGTERM, shutting down server.")
        self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        msg = decode_msg(raw_message)
        # self.logger.custom(f"Nodo {self.id} le llego el mensaje {msg} por la cola {self.coordination_queue}")
        if msg.type == MsgType.FIN:
            self.logger.custom(f"Nodo {self.id} era un FIN")
            self.fins_counter += 1
            if self.fins_counter == self.n_nodes:
                if self.id == 1:
                    # Reenvía el mensaje FIN y cierra la conexión
                    # self.logger.custom(f"Soy el nodo lider {self.id}, mando los FINs")
                    for node, n_nodes in self.n_next_nodes:
                        for _ in range(n_nodes):
                            if node == 'RELEASE_DATE':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_Q2GAMES)
                            if node == 'JOINER_Q3':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_BASICGAMES)
                            if node == 'SHOOTER':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)
                        # self.logger.custom(f"Le mande {n_nodes} FINs a {node}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self._shutdown()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        def process_message(ch, method, properties, raw_message):
            """Callback para procesar el mensaje de la cola."""
            msg = decode_msg(raw_message)
            
            if msg.type == MsgType.GAMES:
                indie_basic_games = []
                indie_q2_games = []
                shooter_games = []
                
                for game in msg.games:
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

                self._middleware.channel.stop_consuming()
                # Reenvía el mensaje FIN a otros nodos y finaliza
                if self.n_nodes > 1:
                    key = f"coordination_{self.id}"
                    self._middleware.send_to_queue(E_COORD_GENRE, msg.encode(), key=key)
                else:
                    for node, n_nodes in self.n_next_nodes:
                        for _ in range(n_nodes):
                            if node == 'RELEASE_DATE':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_Q2GAMES)
                            if node == 'JOINER_Q3':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_INDIE_BASICGAMES)
                            if node == 'SHOOTER':
                                self._middleware.send_to_queue(E_FROM_GENRE, msg.encode(), key=K_SHOOTER_GAMES)

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_TRIMMER_GENRE_FILTER, process_message)
            if self.n_nodes > 1:
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)
            else:
                self._shutdown()
            
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")

