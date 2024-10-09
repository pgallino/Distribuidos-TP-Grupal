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
        return Score.POSITIVE if score == "1" else Score.NEGATIVE

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

# ┌────────────────────────────┐
# │       ESTRUCTURA DE        │
# │          MENSAJES          │
# └────────────────────────────┘

# ┌────────── Mensaje Base ──────────┐
# │ 1 byte  : tipo        (MsgType)  │
# │ 1 byte  : id          (id)       │
# └──────────────────────────────────┘

# ┌──────────── HANDSHAKE ───────────┐
# │ 1 byte  : tipo       (HANDSHAKE) │
# │ 1 byte  : id         (id)        │
# └──────────────────────────────────┘

# ┌─────────────── DATA ──────────────┐
# │ 1 byte  : tipo       (DATA)       │
# │ 1 byte  : id         (id)         │
# │ 1 byte  : dataset    (GAME/REVIEW)│
# │ 4 bytes : longitud   (data_len)   │
# │ N bytes : filas      (UTF-8)      │
# └───────────────────────────────────┘

# ┌─────────────── FIN ──────────────┐
# │ 1 byte  : tipo       (FIN)       │
# │ 1 byte  : id         (id)        │
# └──────────────────────────────────┘

# ┌────────────── GAME ──────────────┐
# │ 4 bytes : app_id                 │
# │ 1 byte  : long. nombre           │
# │ N bytes : nombre     (UTF-8)     │
# │ 1 byte  : long. fecha            │
# │ N bytes : fecha      (UTF-8)     │
# │ 4 bytes : avg_playtime           │
# │ 1 byte  : compatib. (win/lin/mac)│
# │ 1 byte  : géneros                │
# │ N bytes : lista de géneros       │
# └──────────────────────────────────┘

# ┌────────────── GAMES ─────────────┐
# │ 1 byte  : tipo       (GAMES)     │
# │ 1 byte  : id         (id)        │
# │ 2 bytes : juegos                 │
# │ N bytes : lista      (Game)      │
# └──────────────────────────────────┘

# ┌───────────── REVIEW ─────────────┐
# │ 4 bytes : app_id                 │
# │ 2 bytes : long. texto            │
# │ N bytes : texto      (UTF-8)     │
# │ 1 byte  : score      (pos/neg)   │
# └──────────────────────────────────┘

# ┌──────────── REVIEWS ─────────────┐
# │ 1 byte  : tipo       (REVIEWS)   │
# │ 1 byte  : id         (id)        │
# │ 4 bytes : reseñas                │
# │ N bytes : lista      (Review)    │
# └──────────────────────────────────┘

# ┌───────────── RESULT ─────────────┐
# │ 1 byte  : tipo       (RESULT)    │
# │ 1 byte  : id         (id)        │
# │ 1 byte  : query_num              │
# │ 2 bytes : long. res  (result_len)│
# │ N bytes : resultado  (UTF-8)     │
# └──────────────────────────────────┘



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
        return Reviews.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif msg_type == MsgType.RESULT:
        return Result.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

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
        
        return body

    @staticmethod
    def decode(data: bytes) -> "Review":
        """
        Decodifica bytes en un objeto `Review`.
        """
        offset = 0
        app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
        offset += 6

        text = data[offset:offset + text_length].decode('utf-8')
        offset += text_length
        
        score_value = data[offset]
        try:
            score = Score(score_value)
        except ValueError:
            raise ValueError(f"Valor de score desconocido: {score_value}")
        
        return Review(app_id, text, score)

    def __str__(self):
        return f"Review(app_id={self.app_id}, text={self.text}, score={self.score})"


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

        # Empaquetar tipo de mensaje, id, número de reseñas, y los datos de reseñas
        body = struct.pack('>BBI', int(MsgType.REVIEWS.value), self.id, reviews_count) + reviews_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Reviews":
        """
        Decodifica bytes en un mensaje `Reviews` que contiene múltiples objetos `Review`.
        """
        offset = 0
        id, reviews_count = struct.unpack('>BI', data[:5])
        offset += 5

        reviews = []
        for _ in range(reviews_count):
            # Decodifica cada `Review` y avanza el offset
            app_id, text_length = struct.unpack('>IH', data[offset:offset + 6])
            offset += 6

            text = data[offset:offset + text_length].decode('utf-8')
            offset += text_length
            
            score_value = data[offset]
            try:
                score = Score(score_value)
            except ValueError:
                raise ValueError(f"Valor de score desconocido: {score_value}")
            
            offset += 1
            review = Review(app_id, text, score)
            reviews.append(review)

        return Reviews(id, reviews)

    def __str__(self):
        return f"Reviews(id={self.id}, reviews={[str(review) for review in self.reviews]})"

# ===================================================================================================================== #

class Result(Message):

    def __init__(self, id: int, query_number: int, result: str):
        super().__init__(id, MsgType.RESULT)
        self.query_number = query_number
        self.result = result

    def encode(self) -> bytes:
        # Codifica el mensaje Result
        result_bytes = self.result.encode()
        result_length = len(result_bytes)

        # Empaquetamos los campos según la estructura definida
        body = struct.pack(f'>BBBH{result_length}s',
                           int(MsgType.RESULT.value),  # 1 byte para el tipo de mensaje
                           self.id,                   # 1 byte para el id
                           self.query_number,         # 1 byte para el query_number
                           result_length,             # 2 bytes para el len_string
                           result_bytes)              # N bytes para el result

        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)

        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Result":
        # Decodifica el mensaje Result
        id, query_number, result_length = struct.unpack('>BBH', data[:4])
        result = data[4:4 + result_length].decode()
        return Result(id, query_number, result)
    
    def __str__(self):
        return f"Result(id={self.id}, query_number={self.query_number}, result={self.result})"

# ===================================================================================================================== #

# ===================================================================================================================== #

class ResultQ1(Message):

    def __init__(self, id: int, windows: int, mac: int, linux: int):
        super().__init__(id, MsgType.RESULTQ1)
        self.windows = windows
        self.mac = mac
        self.linux = linux

    def encode(self) -> bytes:
        # Codifica el mensaje Review

        # Convertimos los datos a bytes
        body = struct.pack(f'>BBIII', int(MsgType.RESULTQ1.value), self.id, self.windows, self.mac, self.linux)
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body



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
