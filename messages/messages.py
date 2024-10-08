from enum import Enum
import struct
from typing import List

# Definición de los tipos de mensajes
class MsgType(Enum):
    HANDSHAKE = 0
    DATA = 1
    FIN = 2
    GAME = 3
    REVIEW = 4
    RESULT = 5

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


# =========================================

# Estructura de los Mensajes:

# Handshake:
# Byte 1: Tipo de mensaje (1 byte).
# Byte 2: ID del mensaje (1 byte).

# Fin:
# Byte 1: Tipo de mensaje (1 byte).
# Byte 2: ID del mensaje (1 byte).

# Data:
# Byte 1: Tipo de mensaje (1 byte).
# Byte 2: ID del mensaje (1 byte).
# Byte 3: Dataset origen (1 byte).
# Byte 4-5: Largo de los datos (2 bytes).
# Byte 6-N: Datos (cadena codificada).

# =========================================


def decode_msg(data):
    """
    Decodifica un mensaje recibido y devuelve una instancia de la clase correspondiente.
    """
    type = MsgType(data[0])

    if type == MsgType.HANDSHAKE:
        return Handshake.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif type == MsgType.DATA:
        return Data.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif type == MsgType.FIN:
        return Fin.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)
    
    elif type == MsgType.GAME:
        return Game.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)
    
    elif type == MsgType.REVIEW:
        return Review.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

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

class Handshake(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.HANDSHAKE)

    def encode(self) -> bytes:
        # Codifica el mensaje Handshake
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', int(MsgType.HANDSHAKE.value), self.id)
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> 'Handshake':
        # Decodifica el mensaje Handshake
        id = struct.unpack('>B', data[:1])[0]
        return Handshake(id)
    
    def __str__(self):
        return f"Handshake(id={self.id})"

class Data(Message):
    def __init__(self, id: int, row: str, dataset: Dataset):
        super().__init__(id, MsgType.DATA)
        self.row = row
        self.dataset = dataset
    
    def encode(self) -> bytes:
        # Codifica el mensaje Data
        # Convertimos los datos a bytes
        data_bytes = self.row.encode()
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el ID, el dataset y la longitud de los datos (2 bytes)
        body = struct.pack('>BBBH', int(MsgType.DATA.value), self.id, self.dataset.value, data_length) + data_bytes
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body

    @staticmethod
    def decode(data: bytes) -> 'Data':
        # Decodifica el mensaje Data
        id, dataset, data_length = struct.unpack('>BBH', data[:4])
        data_str = data[4:4+data_length].decode()
        return Data(id, data_str, Dataset(dataset))

    def __str__(self):
        return f"Data(id={self.id}, row={self.row}')"

class Fin(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.FIN)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', int(MsgType.FIN.value), self.id)
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body

    @staticmethod
    def decode(data: bytes) -> 'Fin':
        # Decodifica el mensaje Fin
        id = struct.unpack('>B', data[:1])[0]
        return Fin(id)

    def __str__(self):
        return f"Fin(id={self.id})"
    
class Game(Message):

    def __init__(self, id: int, app_id: int, name: str, release_date: str, avg_playtime: int, windows: bool, linux: bool, mac: bool, genres: List[Genre]):
        super().__init__(id, MsgType.GAME)
        self.app_id = app_id
        self.name = name
        self.genres = genres
        self.release_date = release_date
        self.avg_playtime = avg_playtime
        self.windows = windows
        self.linux = linux
        self.mac = mac

    def encode(self) -> bytes:
        # Codifica el mensaje Game

        # Convertimos los datos a bytes
        name_bytes = self.name.encode()
        release_date_bytes = self.release_date.encode()

        genres_bytes = [genre.value.to_bytes(1, "big") for genre in self.genres]

        body = struct.pack(f'>BBIB{len(name_bytes)}sB{len(release_date_bytes)}sI???B',
                           int(MsgType.GAME.value), 
                           self.id, 
                           self.app_id, 
                           len(name_bytes), 
                           name_bytes, 
                           len(release_date_bytes), 
                           release_date_bytes, 
                           self.avg_playtime, 
                           self.windows, 
                           self.linux, 
                           self.mac, 
                           len(genres_bytes)
                           ) + b''.join(genres_bytes)
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> "Game":
        # Decodifica el mensaje Game
        init, end = 0, 6
        # print(data[init:end])
        id, app_id, name_length = struct.unpack('>BIB', data[:end])
        init, end = end, end + name_length
        # print(data[init:end])
        name = data[init:end].decode()
        init, end = end, end + 1
        # print(data[init:end])
        release_date_length, = struct.unpack('>B', data[init:end])
        init, end = end, end + release_date_length
        # print(data[init:end])
        release_date = data[init:end].decode()
        init, end = end, end + 8
        # print(data[init:end])
        avg_playtime, windows, linux, mac, genres_length = struct.unpack('>IBBBB', data[init:end])
        init, end = end, end + genres_length
        # print(data[init:end])
        genres = [Genre(int(genre)) for genre in data[init:end]]
        return Game(id, app_id, name, release_date, avg_playtime, windows, linux, mac, genres)
    
    def __str__(self):
        return f"Game(id={self.id}, app_id={self.app_id}, name={self.name}, genre={self.genres}, release_date={self.release_date}, avg_playtime={self.avg_playtime}, windows={self.windows}, linux={self.linux}, mac={self.mac})"
    
class Review(Message):

    def __init__(self, id: int, app_id: int, text: str, score: Score):
        super().__init__(id, MsgType.REVIEW)
        self.app_id = app_id
        self.text = text
        self.score = score

    def encode(self) -> bytes:
        # Codifica el mensaje Review

        # Convertimos los datos a bytes
        text_bytes = self.text.encode()
        
        body = struct.pack(f'>BBII{len(text_bytes)}sB', int(MsgType.REVIEW.value), self.id, self.app_id, len(text_bytes), text_bytes, self.score.value)
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> "Review":
        # Decodifica el mensaje Review
        init, end = 0, 9
        id, app_id, name_length = struct.unpack('>BII', data[:end])
        init, end = end, end + name_length
        text = data[init:end].decode()
        init, end = end, end + 1
        score, = struct.unpack('>B', data[init:end])
        return Review(id, app_id, text, Score(score))
    
    def __str__(self):
        return f"Review(id={self.id}, app_id={self.app_id}, score={self.score})"

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

        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)

        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Result":
        # Decodifica el mensaje Result
        id, query_number, result_length = struct.unpack('>BBH', data[:4])
        result = data[4:4 + result_length].decode()
        return Result(id, query_number, result)
    
    def __str__(self):
        return f"Result(id={self.id}, query_number={self.query_number}, result={self.result})"