from enum import Enum
import struct
from typing import List

BATCH_SIZE = 10000

# Definición de los tipos de mensajes
class MsgType(Enum):
    HANDSHAKE = 0
    DATA = 1
    FIN = 2
    GAMES = 3
    REVIEWS = 4
    RESULT = 5
    RESULTQ1 = 6

class GamesType(Enum):
    Q1GAMES = 0
    Q2GAMES = 1
    BASICGAME = 2
    GENREGAMES = 3

class ReviewsType(Enum):
    FULLREVIEW = 0
    BASICREVIEW = 1
    TEXTREVIEW = 2

class Dataset(Enum):
    GAME = 0
    REVIEW = 1

class Genre(Enum):
    INDIE = 0
    ACTION = 1
    OTHER = 2

    def from_string(genre: str) -> "Genre":
        if genre == "Indie":
            return Genre.INDIE
        elif genre == "Action":
            return Genre.ACTION
        return Genre.OTHER

class Score(Enum):
    POSITIVE = 0
    NEGATIVE = 1

    def from_string(score: str) -> "Score":
        return Score.POSITIVE if int(score) > 0 else Score.NEGATIVE

class QueryNumber(Enum):
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5

# IMPORTANTE
# IMPORTANTE
# IMPORTANTE   En el encode se agrega el largo total del mensaje primero, en el decode ya no lo tiene
# IMPORTANTE
# IMPORTANTE



def decode_msg(data):
    """
    Decodifica un mensaje recibido y devuelve una instancia de la clase correspondiente.
    """
    # Saltar los primeros 4 bytes de longitud
    data = data[4:]

    msg_type = MsgType(data[0])

    if msg_type == MsgType.HANDSHAKE:
        return Handshake.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif msg_type == MsgType.DATA:
        return Data.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif msg_type == MsgType.FIN:
        return Fin.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)
    
    elif msg_type == MsgType.GAMES:
        # Leer el tipo específico de Games en el segundo byte
        games_type = GamesType(data[1])
        
        # Saltamos los dos primeros bytes (msg_type y games_type) para pasar el resto a la subclase
        if games_type == GamesType.BASICGAME:
            return BasicGames.decode(data[2:])
        elif games_type == GamesType.Q1GAMES:
            return Q1Games.decode(data[2:])
        elif games_type == GamesType.Q2GAMES:
            return Q2Games.decode(data[2:])
        elif games_type == GamesType.GENREGAMES:
            return GenreGames.decode(data[2:])
        else:
            raise ValueError(f"Tipo de Games desconocido: {games_type}")
    
    elif msg_type == MsgType.REVIEWS:
        # Leer el tipo específico de Review en el segundo byte
        review_type = ReviewsType(data[1])

        # Saltamos los dos primeros bytes (msg_type y review_type) para pasar el resto a la subclase
        if review_type == ReviewsType.FULLREVIEW:
            return Reviews.decode(data[2:])
        elif review_type == ReviewsType.BASICREVIEW:
            return BasicReviews.decode(data[2:])
        elif review_type == ReviewsType.TEXTREVIEW:
            return TextReviews.decode(data[2:])
        else:
            raise ValueError(f"Tipo de Review desconocido: {review_type}")

    elif msg_type == MsgType.RESULT:

        # Leer el tipo específico de Result en el segundo byte
        result_type = QueryNumber(data[1])
        
        # Saltamos los dos primeros bytes (msg_type y result) para pasar el resto a la subclase
        if result_type == QueryNumber.Q1:
            return Q1Result.decode(data[2:])
        elif result_type == QueryNumber.Q2:
            return Q2Result.decode(data[2:])
        elif result_type == QueryNumber.Q3:
            return Q3Result.decode(data[2:])
        elif result_type == QueryNumber.Q4:
            return Q4Result.decode(data[2:])
        elif result_type == QueryNumber.Q5:
            return Q5Result.decode(data[2:])
        else:
            raise ValueError(f"Tipo de result desconocido: {result_type}")

    else:
        raise ValueError(f"Tipo de mensaje desconocido: {msg_type}")

class Message:
    def __init__(self, id: int, type: MsgType):
        self.id = id
        self.type = type

    def encode(self) -> bytes:
        raise NotImplementedError("Debe implementarse en las subclases")
    
    @staticmethod
    def decode(data: bytes) -> 'Message':
        raise NotImplementedError("Debe implementarse en las subclases")

    def __str__(self):
        return f"Message(type={self.type})"
    
    def __getattribute__(self, name):
        return super().__getattribute__(name)

# ===================================================================================================================== #

class Handshake(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.HANDSHAKE)

    def encode(self) -> bytes:
        # Codifica el mensaje Handshake
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', int(MsgType.HANDSHAKE.value), self.id)
        
        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> 'Handshake':
        # Decodifica el mensaje Handshake
        id = struct.unpack('>B', data[:1])[0]
        return Handshake(id)
    
    def __str__(self):
        return f"Handshake(id={self.id})"

# ===================================================================================================================== #

class Data(Message):
    def __init__(self, id: int, rows: List[str], dataset: Dataset):
        super().__init__(id, MsgType.DATA)
        self.rows = rows  # Ahora se espera una lista de filas en lugar de una sola fila
        self.dataset = dataset
    
    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el ID, el dataset y la longitud de los datos (4 bytes)
        body = struct.pack('>BBBI', int(MsgType.DATA.value), self.id, self.dataset.value, data_length) + data_bytes
        
        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> 'Data':
        # Decodifica el mensaje Data
        id, dataset, data_length = struct.unpack('>BBI', data[:6])
        
        # Decodifica el bloque completo de datos como un solo string y luego lo separa en filas
        rows_str = data[6:6+data_length].decode()
        rows = rows_str.split("\n")
        
        return Data(id, rows, Dataset(dataset))

    def __str__(self):
        return f"Data(id={self.id}, rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Fin(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.FIN)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', int(MsgType.FIN.value), self.id)
        
        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> 'Fin':
        # Decodifica el mensaje Fin
        id = struct.unpack('>B', data[:1])[0]
        return Fin(id)

    def __str__(self):
        return f"Fin(id={self.id})"

# ===================================================================================================================== #

class Review:
    """
    Representa una reseña individual con sus atributos.
    """
    def __init__(self, app_id: int, text: str, score: Score):
        self.app_id = app_id
        self.text = text
        self.score = score

    def encode(self) -> bytes:
        """
        Codifica un objeto `Review` en bytes para ser utilizado en un mensaje.
        """
        text_bytes = self.text.encode('utf-8')
        text_length = len(text_bytes)

        # Codificación del objeto `Review`
        body = struct.pack(
            f'>IH{len(text_bytes)}sB',
            self.app_id,
            text_length,
            text_bytes,
            self.score.value
        )
        
        # Añadir longitud total al principio del mensaje
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Review":
        """
        Decodifica bytes en un objeto `Review`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        offset = 0

        # Decodificar el app_id y la longitud del texto
        app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
        offset += 6

        # Decodificar el texto
        text = data[offset:offset + text_length].decode('utf-8')
        offset += text_length
        
        # Decodificar el valor del score
        score_value = data[offset]
        try:
            score = Score(score_value)
        except ValueError:
            raise ValueError(f"Valor de score desconocido: {score_value}")
        
        return Review(app_id, text, score)

    def __str__(self):
        return f"Review(app_id={self.app_id}, text={self.text}, score={self.score})"

# ===================================================================================================================== #

class BasicReview:
    """
    Representa una reseña básica solo con `app_id`.
    """
    def __init__(self, app_id: int):
        self.app_id = app_id

    def encode(self) -> bytes:
        """
        Codifica un objeto `BasicReview` en bytes.
        """
        body = struct.pack('>I', self.app_id)
        total_length = len(body)
        return struct.pack('>I', total_length) + body  # Añadir longitud total al principio

    @staticmethod
    def decode(data: bytes) -> "BasicReview":
        """
        Decodifica bytes en un objeto `BasicReview`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        app_id = struct.unpack('>I', data[:4])[0]
        return BasicReview(app_id)

    def __str__(self):
        return f"BasicReview(app_id={self.app_id})"

# ===================================================================================================================== #

class TextReview:
    """
    Representa una reseña con `app_id` y `text`.
    """
    def __init__(self, app_id: int, text: str):
        self.app_id = app_id
        self.text = text

    def encode(self) -> bytes:
        """
        Codifica un objeto `TextReview` en bytes.
        """
        text_bytes = self.text.encode('utf-8')
        text_length = len(text_bytes)

        body = struct.pack(f'>IH{len(text_bytes)}s', self.app_id, text_length, text_bytes)
        total_length = len(body)
        return struct.pack('>I', total_length) + body  # Añadir longitud total al principio

    @staticmethod
    def decode(data: bytes) -> "TextReview":
        """
        Decodifica bytes en un objeto `TextReview`.
        Asume que la longitud del mensaje ya fue leída y excluida.
        """
        offset = 0
        app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
        offset += 6
        text = data[offset:offset + text_length].decode('utf-8')
        return TextReview(app_id, text)

    def __str__(self):
        return f"TextReview(app_id={self.app_id}, text={self.text})"

# ===================================================================================================================== #


class Reviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `Review`.
    """
    def __init__(self, id: int, reviews: List[Review]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `Review` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.FULLREVIEW.value), self.id, reviews_count) + reviews_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Reviews":
        """
        Decodifica bytes en un objeto `Reviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `Review`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `Review` y decodificar
            review_data = data[offset:offset + review_length]
            review = Review.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return Reviews(id, reviews)

    def __str__(self):
        return f"Reviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"

class BasicReviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `BasicReview`.
    """
    def __init__(self, id: int, reviews: List[BasicReview]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `BasicReview` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo de mensaje, tipo de review, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.BASICREVIEW.value), self.id, reviews_count) + reviews_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "BasicReviews":
        """
        Decodifica bytes en un objeto `BasicReviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `BasicReview`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `BasicReview` y decodificar
            review_data = data[offset:offset + review_length]
            review = BasicReview.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return BasicReviews(id, reviews)

    def __str__(self):
        return f"BasicReviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"
    
# ===================================================================================================================== #
    
class TextReviews(Message):
    """
    Clase de mensaje que contiene múltiples objetos `TextReview`.
    """
    def __init__(self, id: int, reviews: List[TextReview]):
        super().__init__(id, MsgType.REVIEWS)
        self.reviews = reviews

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `TextReview` en bytes.
        """
        reviews_bytes = b''.join([review.encode() for review in self.reviews])
        reviews_count = len(self.reviews)

        # Empaqueta el tipo de mensaje, tipo de review, id del cliente, el número de reseñas y los datos de reseñas
        body = struct.pack('>BBBH', int(MsgType.REVIEWS.value), int(ReviewsType.TEXTREVIEW.value), self.id, reviews_count) + reviews_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "TextReviews":
        """
        Decodifica bytes en un objeto `TextReviews`.
        """
        offset = 0

        # Leer el tipo de mensaje, tipo de review, id del cliente y el número de reseñas
        id, reviews_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        reviews = []
        for _ in range(reviews_count):
            # Leer la longitud de cada `TextReview`
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos de cada `TextReview` y decodificar
            review_data = data[offset:offset + review_length]
            review = TextReview.decode(review_data)
            reviews.append(review)

            # Avanzar el offset después de extraer la reseña
            offset += review_length

        return TextReviews(id, reviews)

    def __str__(self):
        return f"TextReviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"


# ===================================================================================================================== #


class Game:
    """
    Clase base abstracta que representa un juego solo con `app_id`.
    """
    def __init__(self, app_id: int):
        self.app_id = app_id

    def encode(self) -> bytes:
        """
        Método abstracto de codificación que debe implementarse en subclases.
        """
        raise NotImplementedError("El método encode() debe ser implementado por las subclases de Game")

    @staticmethod
    def decode(data: bytes) -> "Game":
        """
        Método abstracto de decodificación que debe implementarse en subclases.
        """
        raise NotImplementedError("El método decode() debe ser implementado por las subclases de Game")

    def __str__(self):
        return f"Game(app_id={self.app_id})"

# ===================================================================================================================== #

class BasicGame(Game):
    """
    Clase que hereda de `Game` y agrega `name`.
    """
    def __init__(self, app_id: int, name: str):
        super().__init__(app_id)
        self.name = name

    def encode(self) -> bytes:
        """
        Codifica `BasicGame` en bytes, incluyendo la longitud total.
        """
        name_bytes = self.name.encode('utf-8')
        body = struct.pack(f'>I B{len(name_bytes)}s', self.app_id, len(name_bytes), name_bytes)
        return struct.pack('>I', len(body)) + body  # Incluye la longitud total

    @staticmethod
    def decode(data: bytes) -> "BasicGame":
        """
        Decodifica bytes en un objeto `BasicGame`.
        """
        app_id, name_length = struct.unpack('>I B', data[:5])
        name = data[5:5 + name_length].decode('utf-8')
        return BasicGame(app_id, name)

    def __str__(self):
        return f"BasicGame(app_id={self.app_id}, name={self.name})"

# ===================================================================================================================== #

class Q1Game(Game):
    """
    Clase que hereda de `Game` y agrega compatibilidad con plataformas (Windows, Linux, Mac).
    """
    def __init__(self, app_id: int, windows: bool, linux: bool, mac: bool):
        super().__init__(app_id)
        self.windows = windows
        self.linux = linux
        self.mac = mac

    def encode(self) -> bytes:
        """
        Codifica `Q1Game` en bytes, incluyendo la longitud total.
        """
        platforms = self.encode_platforms()
        body = struct.pack(f'>I B', self.app_id, platforms)
        return struct.pack('>I', len(body)) + body  # Incluye la longitud total

    def encode_platforms(self) -> int:
        platforms = 0
        if self.windows:
            platforms |= 0b001
        if self.linux:
            platforms |= 0b010
        if self.mac:
            platforms |= 0b100
        return platforms

    @staticmethod
    def decode(data: bytes) -> "Q1Game":
        app_id, platforms = struct.unpack('>I B', data[:5])
        windows, linux, mac = bool(platforms & 0b001), bool(platforms & 0b010), bool(platforms & 0b100)
        return Q1Game(app_id, windows, linux, mac)

    def __str__(self):
        return f"Q1Game(app_id={self.app_id}, windows={self.windows}, linux={self.linux}, mac={self.mac})"

# ===================================================================================================================== #

class Q2Game(Game):
    """
    Clase que hereda de `Game` y agrega `name`, `release_date` y `avg_playtime`.
    """
    def __init__(self, app_id: int, name: str, release_date: str, avg_playtime: int):
        super().__init__(app_id)
        self.name = name
        self.release_date = release_date
        self.avg_playtime = avg_playtime

    def encode(self) -> bytes:
        """
        Codifica `Q2Game` en bytes, incluyendo la longitud total.
        """
        name_bytes = self.name.encode('utf-8')
        release_date_bytes = self.release_date.encode('utf-8')
        body = struct.pack(
            f'>I B{len(name_bytes)}s B{len(release_date_bytes)}s I',
            self.app_id,
            len(name_bytes),
            name_bytes,
            len(release_date_bytes),
            release_date_bytes,
            self.avg_playtime
        )
        return struct.pack('>I', len(body)) + body  # Incluye la longitud total

    @staticmethod
    def decode(data: bytes) -> "Q2Game":
        app_id, name_length = struct.unpack('>I B', data[:5])
        offset = 5
        name = data[offset:offset + name_length].decode('utf-8')
        offset += name_length

        release_date_length = struct.unpack('>B', data[offset:offset + 1])[0]
        offset += 1
        release_date = data[offset:offset + release_date_length].decode('utf-8')
        offset += release_date_length

        avg_playtime = struct.unpack('>I', data[offset:offset + 4])[0]
        return Q2Game(app_id, name, release_date, avg_playtime)

    def __str__(self):
        return f"Q2Game(app_id={self.app_id}, name={self.name}, release_date={self.release_date}, avg_playtime={self.avg_playtime})"

# ===================================================================================================================== #

class GenreGame(Game):
    """
    Clase que hereda de `Game` y agrega `name`, `release_date`, `avg_playtime`, y `genres`.
    """
    def __init__(self, app_id: int, name: str, release_date: str, avg_playtime: int, genres: List[int]):
        super().__init__(app_id)
        self.name = name
        self.release_date = release_date
        self.avg_playtime = avg_playtime
        self.genres = genres

    def encode(self) -> bytes:
        """
        Codifica `GenreGame` en bytes, incluyendo la longitud total.
        """
        name_bytes = self.name.encode('utf-8')
        release_date_bytes = self.release_date.encode('utf-8')
        genres_bytes = b''.join([struct.pack('B', genre.value) for genre in self.genres])
        
        body = struct.pack(
            f'>I B{len(name_bytes)}s B{len(release_date_bytes)}s I B',
            self.app_id,
            len(name_bytes),
            name_bytes,
            len(release_date_bytes),
            release_date_bytes,
            self.avg_playtime,
            len(self.genres)
        ) + genres_bytes

        return struct.pack('>I', len(body)) + body  # Incluye la longitud total

    @staticmethod
    def decode(data: bytes) -> "GenreGame":
        app_id, name_length = struct.unpack('>I B', data[:5])
        offset = 5
        name = data[offset:offset + name_length].decode('utf-8')
        offset += name_length

        release_date_length = struct.unpack('>B', data[offset:offset + 1])[0]
        offset += 1
        release_date = data[offset:offset + release_date_length].decode('utf-8')
        offset += release_date_length

        avg_playtime, genres_count = struct.unpack('>IB', data[offset:offset + 5])
        offset += 5
        genres = [data[offset + i] for i in range(genres_count)]

        return GenreGame(app_id, name, release_date, avg_playtime, genres)

    def __str__(self):
        return f"GenreGame(app_id={self.app_id}, name={self.name}, release_date={self.release_date}, avg_playtime={self.avg_playtime}, genres={self.genres})"


# ======================================= GAMES

class Games(Message):
    """
    Clase de mensaje que contiene múltiples objetos `Game` (o de sus subclases).
    """
    def __init__(self, id: int, games: List[Game]):
        super().__init__(id, MsgType.GAMES)
        self.games = games

    def encode(self) -> bytes:

        """
        Método abstracto de decodificación que debe implementarse en subclases.
        """
        raise NotImplementedError("El método decode() debe ser implementado por las subclases de Game")

    @staticmethod
    def decode(self) -> "Games":
        """
        Método abstracto de decodificación que debe implementarse en subclases.
        """
        raise NotImplementedError("El método decode() debe ser implementado por las subclases de Game")


    def __str__(self):
        return f"Games(id={self.id}, games={[str(game) for game in self.games]})"

class BasicGames(Games):
    """
    Clase de mensaje que contiene múltiples objetos `BasicGame`.
    """
    def __init__(self, id: int, games: List[BasicGame]):
        super().__init__(id, games)

    def encode(self) -> bytes:

        games_bytes = b''.join([game.encode() for game in self.games])
        games_count = len(self.games)

        # Empaqueta tipo de mensaje, id del cliente, número de juegos, tipo de `Games` y datos de juegos
        body = struct.pack('>BBBH', int(MsgType.GAMES.value), int(GamesType.BASICGAME.value), self.id, games_count) + games_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "BasicGames":
        offset = 0 
        id, games_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        games = []
        for _ in range(games_count):
            game_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos del juego basándonos en `game_length`
            game_data = data[offset:offset + game_length]
            game = BasicGame.decode(game_data)
            games.append(game)

            # Avanzar el offset después de extraer el juego
            offset += game_length

        return BasicGames(id, games)


class Q1Games(Games):
    """
    Clase de mensaje que contiene múltiples objetos `Q1Game`.
    """
    def __init__(self, id: int, games: List[Q1Game]):
        super().__init__(id, games)

    def encode(self) -> bytes:

        games_bytes = b''.join([game.encode() for game in self.games])
        games_count = len(self.games)

        # Empaqueta tipo de mensaje, id del cliente, número de juegos, tipo de `Games` y datos de juegos
        body = struct.pack('>BBBH', int(MsgType.GAMES.value), int(GamesType.Q1GAMES.value), self.id, games_count) + games_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q1Games":
        offset = 0 
        id, games_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        games = []
        for _ in range(games_count):
            game_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos del juego basándonos en `game_length`
            game_data = data[offset:offset + game_length]
            game = Q1Game.decode(game_data)
            games.append(game)

            # Avanzar el offset después de extraer el juego
            offset += game_length

        return Q1Games(id, games)


class Q2Games(Games):
    """
    Clase de mensaje que contiene múltiples objetos `Q2Game`.
    """
    def __init__(self, id: int, games: List[Q2Game]):
        super().__init__(id, games)

    def encode(self) -> bytes:

        games_bytes = b''.join([game.encode() for game in self.games])
        games_count = len(self.games)

        # Empaqueta tipo de mensaje, id del cliente, número de juegos, tipo de `Games` y datos de juegos
        body = struct.pack('>BBBH', int(MsgType.GAMES.value), int(GamesType.Q2GAMES.value), self.id, games_count) + games_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q2Games":
        offset = 0 
        id, games_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        games = []
        for _ in range(games_count):
            game_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos del juego basándonos en `game_length`
            game_data = data[offset:offset + game_length]
            game = Q2Game.decode(game_data)
            games.append(game)

            # Avanzar el offset después de extraer el juego
            offset += game_length

        return Q2Games(id, games)


class GenreGames(Games):
    """
    Clase de mensaje que contiene múltiples objetos `GenreGame`.
    """
    def __init__(self, id: int, games: List[GenreGame]):
        super().__init__(id, games)

    def encode(self) -> bytes:

        games_bytes = b''.join([game.encode() for game in self.games])
        games_count = len(self.games)

        # Empaqueta tipo de mensaje, id del cliente, número de juegos, tipo de `Games` y datos de juegos
        body = struct.pack('>BBBH', int(MsgType.GAMES.value), int(GamesType.GENREGAMES.value), self.id, games_count) + games_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> "GenreGames":
        offset = 0 
        id, games_count = struct.unpack('>BH', data[offset:offset + 3])
        offset += 3

        games = []
        for _ in range(games_count):
            game_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4

            # Extraer los datos del juego basándonos en `game_length`
            game_data = data[offset:offset + game_length]
            game = GenreGame.decode(game_data)
            games.append(game)

            # Avanzar el offset después de extraer el juego
            offset += game_length

        return GenreGames(id, games)



class Result(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.RESULT)

class Q1Result(Result):
    def __init__(self, id: int, windows_count: int, mac_count: int, linux_count: int):
        super().__init__(id)
        self.result_type = QueryNumber.Q1
        self.windows_count = windows_count
        self.mac_count = mac_count
        self.linux_count = linux_count

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, id, y los conteos
        body = struct.pack('>BBBIHH', MsgType.RESULT.value, self.result_type.value, self.id, self.windows_count, self.mac_count, self.linux_count)
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q1Result":
        
        # Desempaqueta el tipo de mensaje, id, y los conteos
        id, windows_count, mac_count, linux_count = struct.unpack('>BIHH', data)
        
        # Retorna una instancia de Q1Result con los valores decodificados
        return Q1Result(id, windows_count, mac_count, linux_count)

class Q2Result(Result):
    def __init__(self, id: int, top_games: list[tuple[str, int]]):
        super().__init__(id)
        self.result_type = QueryNumber.Q2
        self.top_games = top_games

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, result_type, id y la lista de juegos con tiempos de juego
        body = struct.pack('>BBB', MsgType.RESULT.value, self.result_type.value, self.id)
        for name, playtime in self.top_games:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>H{name_length}sI', name_length, name_encoded, playtime)
        
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q2Result":
        # Extrae id y lista de juegos
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        top_games = []
        
        while offset < len(data):
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            playtime = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            top_games.append((name, playtime))
        
        return Q2Result(id, top_games)


class Q3Result(Result):
    def __init__(self, id: int, top_indie_games: list[tuple[str, int]]):
        super().__init__(id)
        self.result_type = QueryNumber.Q3
        self.top_indie_games = top_indie_games

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, result_type, id y la lista de juegos con sus reseñas
        body = struct.pack('>BBB', MsgType.RESULT.value, self.result_type.value, self.id)
        for name, reviews in self.top_indie_games:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>H{name_length}sI', name_length, name_encoded, reviews)
        
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q3Result":
        # Extrae id y lista de juegos con sus reseñas
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        top_indie_games = []
        
        while offset < len(data):
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            reviews = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            top_indie_games.append((name, reviews))
        
        return Q3Result(id, top_indie_games)


class Q4Result(Result):
    def __init__(self, id: int, negative_reviews: list[tuple[str, int]]):
        super().__init__(id)
        self.result_type = QueryNumber.Q4
        self.negative_reviews = negative_reviews

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, result_type, id y la lista de juegos con sus reseñas negativas
        body = struct.pack('>BBB', MsgType.RESULT.value, self.result_type.value, self.id)
        for name, count in self.negative_reviews:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>H{name_length}sI', name_length, name_encoded, count)
        
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q4Result":
        # Extrae id y lista de juegos con sus reseñas negativas
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        negative_reviews = []
        
        while offset < len(data):
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            count = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            negative_reviews.append((name, count))
        
        return Q4Result(id, negative_reviews)


class Q5Result(Result):
    def __init__(self, id: int, top_negative_reviews: list[tuple[int, str, int]]):
        super().__init__(id)
        self.result_type = QueryNumber.Q5
        self.top_negative_reviews = top_negative_reviews

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, result_type, id y la lista de juegos en el percentil superior de reseñas negativas
        body = struct.pack('>BBB', MsgType.RESULT.value, self.result_type.value, self.id)
        for app_id, name, count in self.top_negative_reviews:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>I H{name_length}s I', app_id, name_length, name_encoded, count)
        
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q5Result":
        # Extrae id y lista de juegos con sus reseñas negativas
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        top_negative_reviews = []
        
        while offset < len(data):
            app_id = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            count = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            top_negative_reviews.append((app_id, name, count))
        
        return Q5Result(id, top_negative_reviews)
