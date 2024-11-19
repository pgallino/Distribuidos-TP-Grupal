import struct
from enum import Enum
from typing import List

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

class GamesType(Enum):
    BASICGAME = 0
    Q1GAMES = 1
    Q2GAMES = 2
    GENREGAMES = 3


class Game:
    """
    Clase base abstracta que representa un juego solo con `app_id`.
    """
    def __init__(self, app_id: int):
        self.app_id = app_id

    def encode(self) -> bytes:
        raise NotImplementedError("El método encode() debe ser implementado por las subclases de Game")

    @staticmethod
    def decode(data: bytes) -> "Game":
        raise NotImplementedError("El método decode() debe ser implementado por las subclases de Game")

    def __str__(self):
        return f"Game(app_id={self.app_id})"


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
