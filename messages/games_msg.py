from enum import Enum
from typing import List
from messages.messages import Message, MsgType
import struct

class GamesType(Enum):
    Q1GAMES = 0
    Q2GAMES = 1
    BASICGAME = 2
    GENREGAMES = 3


def decode_game(data: bytes):
    # Leer el tipo específico de Games en el primer byte
    games_type = GamesType(data[0])

    # Saltamos el primer byte (games_type) para pasar el resto a la subclase correspondiente
    if games_type == GamesType.BASICGAME:
        return BasicGames.decode(data[1:])
    elif games_type == GamesType.Q1GAMES:
        return Q1Games.decode(data[1:])
    elif games_type == GamesType.Q2GAMES:
        return Q2Games.decode(data[1:])
    elif games_type == GamesType.GENREGAMES:
        return GenreGames.decode(data[1:])
    else:
        raise ValueError(f"Tipo de Games desconocido: {games_type}")

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