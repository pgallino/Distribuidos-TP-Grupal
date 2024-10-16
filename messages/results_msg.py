from enum import Enum
from messages.messages import Message, MsgType
import struct

class QueryNumber(Enum):
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5

def decode_result(data: bytes):
    # Leer el tipo específico de Result en el primer byte
    result_type = QueryNumber(data[0])

    # Saltamos el primer byte (result_type) para pasar el resto a la subclase correspondiente
    if result_type == QueryNumber.Q1:
        return Q1Result.decode(data[1:])
    elif result_type == QueryNumber.Q2:
        return Q2Result.decode(data[1:])
    elif result_type == QueryNumber.Q3:
        return Q3Result.decode(data[1:])
    elif result_type == QueryNumber.Q4:
        return Q4Result.decode(data[1:])
    elif result_type == QueryNumber.Q5:
        return Q5Result.decode(data[1:])
    else:
        raise ValueError(f"Tipo de result desconocido: {result_type}")

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
    def __init__(self, id: int, negative_reviews: list[tuple[int, str, int]]):
        super().__init__(id)
        self.result_type = QueryNumber.Q4
        self.negative_reviews = negative_reviews

    def encode(self) -> bytes:
        # Empaqueta el tipo de mensaje, result_type, id y la lista de juegos con sus reseñas negativas
        body = struct.pack('>BBB', MsgType.RESULT.value, self.result_type.value, self.id)
        for app_id, name, count in self.negative_reviews:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>IH{name_length}sI', app_id, name_length, name_encoded, count)
        
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> "Q4Result":
        # Extrae id y lista de juegos con sus reseñas negativas
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        negative_reviews = []
        
        while offset < len(data):
            app_id = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            count = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            negative_reviews.append((app_id, name, count))
        
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