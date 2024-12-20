import struct
from enum import Enum

class QueryNumber(Enum):
    """
    Clase enum con las distintas queries.
    """
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5

class Result:
    """
    Clase de resultado genérico de una query.
    """
    def __init__(self):
        raise NotImplementedError("Debe implementarse en subclases de Result")  

    def encode(self) -> bytes:
        raise NotImplementedError("Debe implementarse en subclases de Result")

    @classmethod
    def decode(cls, data: bytes) -> "Result":
        raise NotImplementedError("Debe implementarse en subclases de Result")
    
class Q1Result(Result):
    """
    Clase de los resultados de la query 1.
    """
    def __init__(self, windows_count: int, mac_count: int, linux_count: int):
        """
        Carga los resultados de la query 1.
        """
        self.windows_count = windows_count
        self.mac_count = mac_count
        self.linux_count = linux_count

    def encode(self) -> bytes:
        """Codifica el resultado de la query 1 a bytes para su envío."""
        body = struct.pack('>IHH', self.windows_count, self.mac_count, self.linux_count)
        return body

    @classmethod
    def decode(cls, data: bytes) -> "Q1Result":
        """Decodifica los bytes correspondientes al resultado de la query 1."""
        windows_count, mac_count, linux_count = struct.unpack('>IHH', data)
        return cls(windows_count=windows_count, mac_count=mac_count, linux_count=linux_count)

class Q2Result(Result):
    """
    Clase de los resultados de la query 2.
    """
    def __init__(self, top_games: list[tuple[str, int]]):
        """
        Carga los resultados de la query 2.
        """
        self.top_games = top_games

    def encode(self) -> bytes:
        """Codifica el resultado de la query 2 a bytes para su envío."""
        body = b''
        for name, playtime in self.top_games:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>H{name_length}sI', name_length, name_encoded, playtime)
        return body

    @classmethod
    def decode(cls, data: bytes) -> "Q2Result":
        """Decodifica los bytes correspondientes al resultado de la query 2."""
        offset = 0
        top_games = []
        while offset < len(data):
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            playtime = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            top_games.append((name, playtime))
        return cls(top_games=top_games)

class Q3Result(Result):
    """
    Clase de los resultados de la query 3.
    """
    def __init__(self, top_indie_games: list[tuple[str, int]]):
        """
        Carga los resultados de la query 3.
        """
        self.top_indie_games = top_indie_games

    def encode(self) -> bytes:
        """Codifica el resultado de la query 3 a bytes para su envío."""
        body = b''
        for name, reviews in self.top_indie_games:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>H{name_length}sI', name_length, name_encoded, reviews)
        return body

    @classmethod
    def decode(cls, data: bytes) -> "Q3Result":
        """Decodifica los bytes correspondientes al resultado de la query 3."""
        offset = 0
        top_indie_games = []
        while offset < len(data):
            name_length = struct.unpack('>H', data[offset:offset + 2])[0]
            offset += 2
            name = data[offset:offset + name_length].decode()
            offset += name_length
            reviews = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            top_indie_games.append((name, reviews))
        return cls(top_indie_games=top_indie_games)

class Q4Result(Result):
    """
    Clase de los resultados de la query 4.
    """
    def __init__(self, negative_reviews: list[tuple[int, str, int]]):
        """
        Carga los resultados de la query 4.
        """
        self.negative_reviews = negative_reviews

    def encode(self) -> bytes:
        """Codifica el resultado de la query 4 a bytes para su envío."""
        body = b''
        for app_id, name, count in self.negative_reviews:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>IH{name_length}sI', app_id, name_length, name_encoded, count)
        return body

    @classmethod
    def decode(cls, data: bytes) -> "Q4Result":
        """Decodifica los bytes correspondientes al resultado de la query 4."""
        offset = 0
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
        return cls(negative_reviews=negative_reviews)

class Q5Result(Result):
    """
    Clase de los resultados de la query 5.
    """
    def __init__(self, top_negative_reviews: list[tuple[int, str, int]]):
        """
        Carga los resultados de la query 5.
        """
        self.top_negative_reviews = top_negative_reviews

    def encode(self) -> bytes:
        """Codifica el resultado de la query 5 a bytes para su envío."""
        body = b''
        for app_id, name, count in self.top_negative_reviews:
            name_encoded = name.encode()
            name_length = len(name_encoded)
            body += struct.pack(f'>IH{name_length}sI', app_id, name_length, name_encoded, count)
        return body

    @classmethod
    def decode(cls, data: bytes) -> "Q5Result":
        """Decodifica los bytes correspondientes al resultado de la query 5."""
        offset = 0
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
        return cls(top_negative_reviews=top_negative_reviews)
