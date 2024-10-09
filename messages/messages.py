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

    type = MsgType(data[0])

    if type == MsgType.HANDSHAKE:
        return Handshake.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif type == MsgType.DATA:
        return Data.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif type == MsgType.FIN:
        return Fin.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)
    
    elif type == MsgType.GAMES:
        return Games.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)
    
    elif type == MsgType.REVIEWS:
        return Reviews.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif type == MsgType.RESULT:
        return Result.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    else:
        raise ValueError(f"Tipo de mensaje desconocido: {type}")

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

class Game:
    """
    Representa un juego individual con sus atributos.
    """
    def __init__(self, app_id: int, name: str, release_date: str, avg_playtime: int, windows: bool, linux: bool, mac: bool, genres: List[Genre]):
        self.app_id = app_id
        self.name = name
        self.release_date = release_date
        self.avg_playtime = avg_playtime
        self.windows = windows
        self.linux = linux
        self.mac = mac
        self.genres = genres

    def encode_platforms(self) -> int:
        platforms = 0
        if self.windows:
            platforms |= 0b001  # Activa el bit 0 para Windows
        if self.linux:
            platforms |= 0b010  # Activa el bit 1 para Linux
        if self.mac:
            platforms |= 0b100  # Activa el bit 2 para Mac
        return platforms

    def encode(self) -> bytes:
        """
        Codifica un objeto `Game` en bytes para ser utilizado en un mensaje.
        """
        name_bytes = self.name.encode('utf-8')
        release_date_bytes = self.release_date.encode('utf-8')
        genres_bytes = b''.join([struct.pack('B', genre.value) for genre in self.genres])

        # Codificación del objeto `Game`
        body = struct.pack(
            f'>I B{len(name_bytes)}s B{len(release_date_bytes)}s I B B',
            self.app_id,
            len(name_bytes),
            name_bytes,
            len(release_date_bytes),
            release_date_bytes,
            self.avg_playtime,
            self.encode_platforms(),  # Máscara de bits para la compatibilidad
            len(self.genres)
        ) + genres_bytes

        return body

    @staticmethod
    def decode_platforms(platforms: int) -> tuple:
        windows = bool(platforms & 0b001)  # Verifica el bit 0 para Windows
        linux = bool(platforms & 0b010)    # Verifica el bit 1 para Linux
        mac = bool(platforms & 0b100)      # Verifica el bit 2 para Mac
        return windows, linux, mac

    @staticmethod
    def decode(data: bytes) -> "Game":
        """
        Decodifica bytes en un objeto `Game`.
        """
        offset = 0

        # Desempaqueta los primeros valores
        app_id, name_length = struct.unpack('>I B', data[offset:offset + 5])
        offset += 5

        # Nombre del juego
        name = data[offset:offset + name_length].decode('utf-8')
        offset += name_length

        # Fecha de lanzamiento
        release_date_length = struct.unpack('>B', data[offset:offset + 1])[0]
        offset += 1
        release_date = data[offset:offset + release_date_length].decode('utf-8')
        offset += release_date_length

        # Campos restantes
        avg_playtime, platforms, genres_length = struct.unpack('>I B B', data[offset:offset + 6])
        offset += 6

        # Decodifica la compatibilidad de plataformas
        windows, linux, mac = Game.decode_platforms(platforms)

        # Lista de géneros
        genres = [Genre(data[offset + i]) for i in range(genres_length)]

        return Game(app_id, name, release_date, avg_playtime, windows, linux, mac, genres)

    def __str__(self):
        return f"Game(app_id={self.app_id}, name={self.name}, release_date={self.release_date}, avg_playtime={self.avg_playtime}, windows={self.windows}, linux={self.linux}, mac={self.mac}, genres={self.genres})"

class Games(Message):
    """
    Clase de mensaje que contiene múltiples objetos `Game`.
    """
    def __init__(self, id: int, games: List[Game]):
        super().__init__(id, MsgType.GAMES)
        self.games = games

    def encode(self) -> bytes:
        """
        Codifica una lista de objetos `Game` en bytes.
        """
        games_bytes = b''.join([game.encode() for game in self.games])
        games_count = len(self.games)

        # Empaquetar tipo de mensaje, id del cliente, número de juegos, y los datos de juegos
        body = struct.pack('>BBH', int(MsgType.GAMES.value), self.id, games_count) + games_bytes
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Games":
        """
        Decodifica bytes en un mensaje `Games` que contiene múltiples objetos `Game`.
        """
        offset = 0
        id, games_count = struct.unpack('>BH', data[:3])
        offset += 3

        games = []
        for _ in range(games_count):
            # Decodifica cada juego y avanza el offset

            # app_id y longitud del nombre
            app_id, name_length = struct.unpack('>I B', data[offset:offset + 5])
            offset += 5

            # Verificación de longitud del nombre
            if offset + name_length > len(data):
                raise ValueError("Datos insuficientes para decodificar 'name'.")

            # Nombre del juego
            name = data[offset:offset + name_length].decode('utf-8')
            offset += name_length

            # Longitud de la fecha de lanzamiento y la fecha
            release_date_length = struct.unpack('>B', data[offset:offset + 1])[0]
            offset += 1

            if offset + release_date_length > len(data):
                raise ValueError("Datos insuficientes para decodificar 'release_date'.")

            release_date = data[offset:offset + release_date_length].decode('utf-8')
            offset += release_date_length

            # Campos restantes
            avg_playtime, platforms, genres_length = struct.unpack('>I B B', data[offset:offset + 6])
            offset += 6

            # Decodificar la máscara de bits de plataformas
            windows, linux, mac = Game.decode_platforms(platforms)

            # Validación de longitud de géneros
            if offset + genres_length > len(data):
                raise ValueError("Datos insuficientes para decodificar 'genres'.")

            # Lista de géneros
            genres = [Genre(data[offset + i]) for i in range(genres_length)]
            offset += genres_length

            # Crear el objeto Game y agregarlo a la lista
            game = Game(app_id, name, release_date, avg_playtime, windows, linux, mac, genres)
            games.append(game)

        return Games(id, games)

    def __str__(self):
        return f"Games(id={self.id}, games={[str(game) for game in self.games]})"

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