from messages.messages import Dataset, Genre, MsgType, Review, Score, decode_msg, Reviews, Q1Game, GenreGame, Q1Games, GenreGames
import signal
from typing import List, Tuple
from middleware.middleware import Middleware
import logging
import csv
import sys

GAME_FIELD_NAMES = ['AppID', 'Name', 'Release date', 'Estimated owners', 'Peak CCU', 
                    'Required age', 'Price', 'Unknown', 'DiscountDLC count', 'About the game', 
                    'Supported languages', 'Full audio languages', 'Reviews', 'Header image', 
                    'Website', 'Support url', 'Support email', 'Windows', 'Mac', 
                    'Linux', 'Metacritic score', 'Metacritic url', 'User score', 
                    'Positive', 'Negative', 'Score rank', 'Achievements', 
                    'Recommendations', 'Notes', 'Average playtime forever', 
                    'Average playtime two weeks', 'Median playtime forever', 
                    'Median playtime two weeks', 'Developers', 'Publishers', 
                    'Categories', 'Genres', 'Tags', 'Screenshots', 'Movies']

REVIEW_FIELD_NAMES = ['app_id','app_name','review_text','review_score','review_votes']

Q_GATEWAY_TRIMMER = 'gateway-trimmer'
E_TRIMMER_FILTERS = 'trimmer-filters'
E_COORD_TRIMMER = 'from-coord-trimmer'
K_Q1GAME = 'q1game'
K_GENREGAME = 'genregame'
K_REVIEW = 'review'
Q_COORD_TRIMMER = 'coord-trimmer'

# Aumenta el límite del tamaño de campo
csv.field_size_limit(sys.maxsize)  # Esto establece el límite en el tamaño máximo permitido por el sistema


#### ESTANDAR NOMBRE COLAS ####
# Q_ORIGEN_DESTINO = "origen-destino"

#### ESTANDAR NOMBRE EXCHANGES ####
# E_FROM_ORIGEN = "from_origen"

#### ESTANDAR NOMBRE CLAVES ####
# K_GAME = "game"

def get_genres(genres_string: str):
    values = genres_string.split(',')
    return [genre for value in values if (genre := Genre.from_string(value)) != Genre.OTHER]

class Trimmer:
    def __init__(self, id: int, n_nodes: int, n_next_nodes: List[Tuple[str, int]]):

        self.id = id
        self.n_nodes = n_nodes
        self.n_next_nodes = n_next_nodes
        print(f"{id} -- {n_nodes} -- {n_next_nodes}")
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        
        self._middleware = Middleware()
        self._middleware.declare_queue(Q_GATEWAY_TRIMMER)
        self._middleware.declare_exchange(E_TRIMMER_FILTERS)

        if self.n_nodes > 1:
            self.coordination_queue = Q_COORD_TRIMMER + f"{self.id}"
            self._middleware.declare_queue(self.coordination_queue)
            self._middleware.declare_exchange(E_COORD_TRIMMER)
            for i in range(1, self.n_nodes + 1): # arranco en el id 1 y sigo hasta el numero de nodos
                if i != self.id:
                    routing_key = f"coordination_{i}"
                    self._middleware.bind_queue(self.coordination_queue, E_COORD_TRIMMER, routing_key)
            self.fins_counter = 1

    def _shutdown(self):
        self.shutting_down = True
        self._middleware.connection.close()

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self._shutdown()

    def process_fin(self, ch, method, properties, raw_message):
        self.logger.custom(f"Entre al process fin por {self.n_nodes}")
        msg = decode_msg(raw_message)
        if msg.type == MsgType.FIN:
            self.fins_counter += 1
            if self.id == 1 and self.fins_counter == self.n_nodes:
                # Reenvía el mensaje FIN y cierra la conexión
                self.logger.custom(f"Soy el nodo lider {self.id}, mando los FINs")
                for node, n_nodes in self.n_next_nodes:
                    for _ in range(n_nodes):
                        if node == 'GENRE':
                            self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GENREGAME)
                        if node == 'SCORE':
                            self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                        if node == 'OS_COUNTER':
                            self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_Q1GAME)
                    self.logger.custom(f"Le mande {n_nodes} FINs a {node}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                self.shutting_down = True
                self._middleware.connection.close()
                return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.logger.custom("ENTRE A RUN")

        def process_message(ch, method, properties, raw_message):

            self.logger.custom("ENTRE A PROCESS_MESSAGE")
            """Callback para procesar el mensaje de la cola."""
            genre_games_batch = []
            q1_games_batch = []
            reviews_batch = []
            droped_game_rows = 0
            droped_reviews_rows = 0

            msg = decode_msg(raw_message)
            
            if msg.type == MsgType.DATA:

                self.logger.custom("EL MENSAJE ERA UN DATA")
                
                if msg.dataset == Dataset.GAME:

                    self.logger.custom("EL MENSAJE ERA UN GAME")

                    reader = csv.DictReader(msg.rows, fieldnames=GAME_FIELD_NAMES)
                    for values in reader:
                        q1_game, genre_game = self._get_game(values)
                        if q1_game:
                            q1_games_batch.append(q1_game)
                        if genre_game:
                            genre_games_batch.append(genre_game)
                        if not (q1_game and genre_game):
                            droped_game_rows += 1

                    # Enviar lotes por separado para cada tipo de juego
                    if q1_games_batch:
                        q1_games_msg = Q1Games(msg.id, q1_games_batch)
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, q1_games_msg.encode(), key=K_Q1GAME)
                        q1_games_batch.clear()  # Limpia el batch después de enviar

                    if genre_games_batch:
                        genre_games_msg = GenreGames(msg.id, genre_games_batch)
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, genre_games_msg.encode(), key=K_GENREGAME)
                        genre_games_batch.clear()
                    
                    self.logger.custom("ENVIE TODOS LOS GAMES DEL MENSAJE")

                elif msg.dataset == Dataset.REVIEW:

                    self.logger.custom("EL MENSAJE ERA UNA REVIEW")

                    reader = csv.DictReader(msg.rows, fieldnames=REVIEW_FIELD_NAMES)
                    for values in reader:
                        review = self._get_review(values)
                        if review:
                            reviews_batch.append(review)
                        else:
                            droped_reviews_rows += 1
                    
                    if reviews_batch:
                        reviews_msg = Reviews(msg.id, reviews_batch)
                        self._middleware.send_to_queue(E_TRIMMER_FILTERS, reviews_msg.encode(), key=K_REVIEW)
                        reviews_batch.clear()
                    
                    self.logger.custom("ENVIE TODAS LAS REVIEWS")

            elif msg.type == MsgType.FIN:

                self.logger.custom("EL MENSAJE ERA UN FIN")
                
                # Reenvía el mensaje FIN a otros nodos y cierra la conexión
                self._middleware.channel.stop_consuming()

                self.logger.custom("DEJE DE CONSUMIR CON STOP_CONSUMING BABY")
                
                if self.n_nodes > 1:
                    self.logger.custom(f"COMO TENGO MÁS DE UN NODO {self.n_nodes} ENVIO COORDINATION")
                    self._middleware.send_to_queue(E_COORD_TRIMMER, msg.encode(), key=f"coordination_{self.id}")
                    self.logger.custom("SALI DE ENVIAR EL COORDINATION")
                else:
                    self.logger.custom(f"Solo soy UN nodo {self.id}, Propago el Fin normalmente")
                    for node, n_nodes in self.n_next_nodes:
                        for _ in range(n_nodes):
                            if node == 'GENRE':
                                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_GENREGAME)
                            if node == 'SCORE':
                                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_REVIEW)
                            if node == 'OS_COUNTER':
                                self._middleware.send_to_queue(E_TRIMMER_FILTERS, msg.encode(), key=K_Q1GAME)

                        self.logger.custom(f"Le mande {n_nodes} FINs a {node}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        try:
            # Ejecuta el consumo de mensajes con el callback `process_message`
            self._middleware.receive_from_queue(Q_GATEWAY_TRIMMER, process_message, auto_ack=False)
            self.logger.custom("SALI DE PROCESS_MESSAGE")
            if self.n_nodes > 1:
                self.logger.custom(f"COMO TENGO MÁS DE UN NODO {self.n_nodes} ESCUCHO COORDINATION")
                self._middleware.receive_from_queue(self.coordination_queue, self.process_fin, auto_ack=False)
                self.logger.custom("SALI DE PROCESS_FIN")
            else:
                self.shutting_down = True
                self._middleware.connection.close()
            
        except Exception as e:
            if not self.shutting_down:
                self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")


            
    def _get_game(self, values):
        """
        Crea una instancia de Q1Game y/o GenreGame a partir de los datos, descartando aquellas filas con valores vacíos.
        """
        # Claves necesarias
        required_keys = ['AppID', 'Name', 'Windows', 'Mac', 'Linux', 'Genres', 'Release date', 'Average playtime forever', 'Positive', 'Negative']
        
        # Verificar si alguno de los valores críticos está ausente o es una cadena vacía
        for key in required_keys:
            if key not in values or values[key].strip() == "":
                # print(f"Valor faltante o vacío para la clave: {key}")
                return None, None

        try:
            app_id = int(values['AppID'])
            name = values['Name']
            release_date = values['Release date']
            avg_playtime = int(values['Average playtime forever'])
            windows = values['Windows'] == "True"
            mac = values['Mac'] == "True"
            linux = values['Linux'] == "True"
            genres = get_genres(values['Genres'])
        except (ValueError, KeyError) as e:
            print(f"Error al procesar los valores en _get_game: {e}")
            return None, None

        # Crear Q1Game con compatibilidad de plataformas
        q1_game = Q1Game(app_id, windows, linux, mac) if any([windows, linux, mac]) else None

        # Crear GenreGame si hay géneros y otros detalles
        genre_game = GenreGame(app_id, name, release_date, avg_playtime, genres) if genres else None

        return q1_game, genre_game


    def _get_review(self, values):
        required_keys = ['app_id', 'review_text', 'review_score']
        for key in required_keys:
            if key not in values or values[key] == "":
                # app_id = int(values["app_id"])
                # score = Score.from_string(values['review_score'])
                # if (app_id == 105600 or app_id == 252950 or app_id == 391540) and (score == Score.POSITIVE):
                    # print(f"Valor faltante o vacío para la clave en _get_review: {key}. Review de juego {app_id} con valor: {values['review_text']}")
                return None

        try:
            app_id = int(values['app_id'])
            text = values['review_text']
            score = Score.from_string(values['review_score'])
        except (ValueError, KeyError) as e:
            print(f"Error al procesar la reseña en _get_review: {e}")
            return None

        return Review(app_id, text, score)

            