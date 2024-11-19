from enum import Enum
import struct
import json
from typing import List, Type, TypeVar

from messages.games_msg import BasicGame, Game, GamesType, GenreGame, Q1Game, Q2Game
from messages.results_msg import Q1Result, Q2Result, Q3Result, Q4Result, Q5Result, QueryNumber, Result
from messages.reviews_msg import BasicReview, Review, ReviewsType, TextReview
from utils.utils import DecodeError, handle_encode_error

# Definición de los tipos de mensajes
class MsgType(Enum):
    HANDSHAKE = 0
    DATA = 1
    FIN = 2
    GAMES = 3
    REVIEWS = 4
    RESULT = 5
    COORDFIN = 6
    CLIENT_DATA = 7
    CLIENT_FIN = 8
    PUSH_DATA = 9
    PULL_DATA = 10
    KEEP_ALIVE = 11
    ALIVE = 12

class Dataset(Enum):
    GAME = 0
    REVIEW = 1

T = TypeVar("T", bound="BaseMessage")

# Diccionarios de mapeo para clases específicas
GAME_CLASSES = {
    0: BasicGame,
    1: Q1Game,
    2: Q2Game,
    3: GenreGame
}

REVIEW_CLASSES = {
    0: Review,
    1: BasicReview,
    2: TextReview
}

RESULT_CLASSES = {
    1: Q1Result,
    2: Q2Result,
    3: Q3Result,
    4: Q4Result,
    5: Q5Result
}

# IMPORTANTE ⚠️
# IMPORTANTE ⚠️
# IMPORTANTE ⚠️  En el encode se agrega el largo total del mensaje primero (para los mensajes que son de socket) y el tipo de mensaje, y en el decode ya no los tienen
# IMPORTANTE ⚠️
# IMPORTANTE ⚠️

class BaseMessage:
    def __init__(self, type: MsgType, **kwargs):
        self.type = type
        for key, value in kwargs.items():
            setattr(self, key, value)

    def encode(self) -> bytes:
        """Codifica el mensaje en binario."""
        raise NotImplementedError("Debe implementarse en subclases")

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """Decodifica un mensaje desde binario."""
        raise NotImplementedError("Debe implementarse en subclases")

    def __str__(self):
        return f"{self.__class__.__name__}({vars(self)})"
    
# ===================================================================================================================== #
    
class SimpleMessage(BaseMessage):
    @handle_encode_error
    def encode(self) -> bytes:
        body = struct.pack('>B', int(self.type.value))
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        return cls()  # No pasa `type` porque es inherente a la clase
    
class Handshake(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.HANDSHAKE)

class KeepAlive(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.KEEP_ALIVE)

class Alive(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.ALIVE)

class ClientFin(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.CLIENT_FIN)

class PullData(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.PULL_DATA)

    @handle_encode_error
    def encode(self) -> bytes:
        # Codifica solo el tipo de mensaje sin incluir el total_length
        return struct.pack('>B', int(self.type.value))

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        # Decodifica el mensaje sin esperar el total_length
        return cls()  # No necesita type porque es inherente a la clase


# ===================================================================================================================== #

class ClientData(BaseMessage):
    def __init__(self, rows: List[str], dataset: Dataset):
        super().__init__(MsgType.CLIENT_DATA, rows=rows, dataset=dataset)

    @handle_encode_error
    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)

        # Empaquetamos el tipo de mensaje, el dataset y la longitud de los datos
        body = struct.pack('>BBI', int(self.type.value), self.dataset.value, data_length) + data_bytes

        # Calcular la longitud total del mensaje
        total_length = len(body)

        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 5:
            raise DecodeError("Insufficient data to decode ClientData: header too short")
        dataset, data_length = struct.unpack('>BI', data[:5])
        if len(data) < 5 + data_length:
            raise DecodeError(f"Insufficient data to decode ClientData: expected {5 + data_length}, got {len(data)}")
        rows_str = data[5:5 + data_length].decode()
        rows = rows_str.split("\n")
        return cls(rows=rows, dataset=Dataset(dataset))

    def __str__(self):
        return f"ClientData(rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Data(BaseMessage):
    def __init__(self, id: int, rows: List[str], dataset: Dataset):
        super().__init__(MsgType.DATA, id=id, rows=rows, dataset=dataset)

    @handle_encode_error
    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el ID, el dataset y la longitud de los datos (4 bytes)
        return struct.pack('>BBBI', MsgType.DATA.value, self.id, self.dataset.value, data_length) + data_bytes

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 6:
            raise DecodeError("Insufficient data to decode Data: header too short")
        id, dataset, data_length = struct.unpack('>BBI', data[:6])
        if len(data) < 6 + data_length:
            raise DecodeError(f"Insufficient data to decode Data: expected {6 + data_length}, got {len(data)}")
        rows_str = data[6:6 + data_length].decode()
        rows = rows_str.split("\n")
        return cls(id=id, rows=rows, dataset=Dataset(dataset))

    def __str__(self):
        return f"Data(id={self.id}, rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Fin(BaseMessage):
    def __init__(self, id: int):
        super().__init__(MsgType.FIN, id=id)

    @handle_encode_error
    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID
        return struct.pack('>BB', int(self.type.value), self.id)

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 1:
            raise DecodeError("Insufficient data to decode Fin")
        id = struct.unpack('>B', data[:1])[0]
        return cls(id=id)

    def __str__(self):
        return f"Fin(id={self.id})"

# ===================================================================================================================== #
   
class CoordFin(BaseMessage):
    def __init__(self, id: int, node_id: int):
        super().__init__(MsgType.COORDFIN, id=id, node_id=node_id)

    @handle_encode_error
    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BBB', int(self.type.value), self.id, self.node_id)
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 2:
            raise DecodeError("Insufficient data to decode CoordFin")
        id, node_id = struct.unpack('>BB', data[:2])
        return cls(id=id, node_id=node_id)

    def __str__(self):
        return f"CoordFin(id={self.id}, node_id={self.node_id})"

# ===================================================================================================================== #
    
import json

class PushDataMessage(BaseMessage):
    def __init__(self, data: dict):
        """
        Mensaje genérico para enviar datos arbitrarios entre nodos y réplicas.
        :param data: Diccionario con los datos a enviar.
        """
        super().__init__(MsgType.PUSH_DATA, data=data)
        self.data = data  # Diccionario genérico para almacenar los datos

    @handle_encode_error
    def encode(self) -> bytes:
        """
        Codifica el mensaje en formato binario.
        """
        # Serializar el diccionario de datos a JSON
        data_json = json.dumps(self.data)
        data_bytes = data_json.encode()

        # Empaquetar encabezado: tipo de mensaje y longitud de los datos
        body = struct.pack(
            '>BI', int(self.type.value), len(data_bytes)
        ) + data_bytes

        # Empaquetar el largo total seguido del cuerpo
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 4:
            raise DecodeError("Insufficient data to decode PushDataMessage: header too short")
        data_len = struct.unpack('>I', data[:4])[0]
        if len(data) < 4 + data_len:
            raise DecodeError(f"Insufficient data to decode PushDataMessage: expected {4 + data_len}, got {len(data)}")
        data_json = data[4:4 + data_len].decode()
        parsed_data = json.loads(data_json)
        return cls(data=parsed_data)

    def __str__(self):
        return f"PushDataMessage(data={self.data})"
    
# ===================================================================================================================== #
    
class GamesMessage(BaseMessage):
    def __init__(self, type: MsgType, game_type: GamesType, games: List[Game], id: int):
        super().__init__(type, games=games, game_type=game_type, id=id)

    @handle_encode_error
    def encode(self) -> bytes:
        games_bytes = b"".join([game.encode() for game in self.games])
        # Empaquetar el tipo de mensaje, el tipo de juego, y la cantidad de juegos
        body = struct.pack('>BBBH', int(self.type.value), self.game_type.value, self.id, len(self.games)) + games_bytes
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes, game_type, game_cls: Type[Game]) -> T:
        if len(data) < 3:
            raise DecodeError("Insufficient data to decode GamesMessage: header too short")
        id, games_count = struct.unpack('>BH', data[:3])
        games = decode_items(data[3:], games_count, game_cls)
        return cls(MsgType.GAMES, GamesType(game_type), games, id=id)


# Para las subclases específicas
class BasicGamesMessage(GamesMessage):
    def __init__(self, games: List[BasicGame], id: int):
        super().__init__(MsgType.GAMES, GamesType.BASICGAME, games, id)

class Q1GamesMessage(GamesMessage):
    def __init__(self, games: List[Q1Game], id: int):
        super().__init__(MsgType.GAMES, GamesType.Q1GAMES, games, id)

class Q2GamesMessage(GamesMessage):
    def __init__(self, games: List[Q2Game], id: int):
        super().__init__(MsgType.GAMES, GamesType.Q2GAMES, games, id)

class GenreGamesMessage(GamesMessage):
    def __init__(self, games: List[GenreGame], id: int):
        super().__init__(MsgType.GAMES, GamesType.GENREGAMES, games, id)

# ================================================================================================== #

class ReviewsMessage(BaseMessage):
    def __init__(self, type: MsgType, review_type: ReviewsType, reviews: List[Review], id: int):
        super().__init__(type, reviews=reviews, review_type=review_type, id=id)

    @handle_encode_error
    def encode(self) -> bytes:
        reviews_bytes = b"".join([review.encode() for review in self.reviews])
        # Empaquetar el tipo de mensaje, el tipo de reseña, y la cantidad de reseñas
        body = struct.pack('>BBBH', int(self.type.value), self.review_type.value, self.id, len(self.reviews)) + reviews_bytes
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes, review_type, review_cls: Type[Review]) -> T:
        if len(data) < 3:
            raise DecodeError("Insufficient data to decode ReviewMessage: header too short")
        id, reviews_count = struct.unpack('>BH', data[:3])
        reviews = decode_items(data[3:], reviews_count, review_cls)
        return cls(MsgType.REVIEWS, ReviewsType(review_type), reviews, id=id)

# Para las subclases específicas
class FullReviewsMessage(ReviewsMessage):
    def __init__(self, reviews: List[Review], id: int):
        super().__init__(MsgType.REVIEWS, ReviewsType.FULLREVIEW, reviews, id)

class BasicReviewsMessage(ReviewsMessage):
    def __init__(self, reviews: List[BasicReview], id: int):
        super().__init__(MsgType.REVIEWS, ReviewsType.BASICREVIEW, reviews, id)

class TextReviewsMessage(ReviewsMessage):
    def __init__(self, reviews: List[TextReview], id: int):
        super().__init__(MsgType.REVIEWS, ReviewsType.TEXTREVIEW, reviews, id)

# ========================================================================================================== #

class ResultMessage(BaseMessage):
    def __init__(self, id: int, result_type: QueryNumber, result: Result):
        super().__init__(MsgType.RESULT, id=id, result_type=result_type, result=result)

    @handle_encode_error
    def encode(self) -> bytes:
        # Codifica el tipo de mensaje seguido del tipo de resultado y el resultado específico
        body = struct.pack('>BBB', int(self.type.value), int(self.result_type.value), self.id) + self.result.encode()
        # Empaqueta la longitud del mensaje
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes, result_type, result_cls: Type[Result]) -> T:
        if len(data) < 1:
            raise DecodeError("Insufficient data to decode ResultMessage: header too short")
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        result = result_cls.decode(data[offset:])
        return cls(id=id, result_type=QueryNumber(result_type), result=result)

# ========================================================================================================== #

def decode_items(data: bytes, count: int, item_cls: Type[T]) -> List[T]:
    offset = 0
    items = []
    for _ in range(count):
        if len(data) < offset + 4:
            raise DecodeError("Insufficient data to decode item length")
        item_length = struct.unpack('>I', data[offset:offset + 4])[0]
        offset += 4
        if len(data) < offset + item_length:
            raise DecodeError("Insufficient data to decode item data")
        item_data = data[offset:offset + item_length]
        offset += item_length
        items.append(item_cls.decode(item_data))
    return items


# Uso General del Decode
MESSAGE_CLASSES = {
    MsgType.HANDSHAKE: Handshake,
    MsgType.FIN: Fin,
    MsgType.KEEP_ALIVE: KeepAlive,
    MsgType.ALIVE: Alive,
    MsgType.DATA: Data,
    MsgType.PUSH_DATA: PushDataMessage,
    MsgType.PULL_DATA: PullData,
    MsgType.GAMES: GamesMessage,
    MsgType.REVIEWS: ReviewsMessage,
    MsgType.RESULT: ResultMessage,
    MsgType.CLIENT_DATA: ClientData,
    MsgType.CLIENT_FIN: ClientFin,
    MsgType.COORDFIN: CoordFin,
}

def decode_msg(data: bytes):
    try:
        type = MsgType(data[0])
    except IndexError:
        raise DecodeError("Data too short to determine message type")
    except ValueError:
        raise DecodeError(f"Unknown MsgType: {data[0]}")

    if type in [MsgType.GAMES, MsgType.REVIEWS, MsgType.RESULT]:
        cls_map = {
            MsgType.GAMES: GAME_CLASSES,
            MsgType.REVIEWS: REVIEW_CLASSES,
            MsgType.RESULT: RESULT_CLASSES,
        }
        try:
            sub_cls_map = cls_map[type]
            sub_type = data[1]
            sub_cls = sub_cls_map[sub_type]
            return MESSAGE_CLASSES[type].decode(data[2:], sub_type, sub_cls)
        except KeyError:
            raise DecodeError(f"Unknown subtype {data[1]} for MsgType {type}")

    elif type in MESSAGE_CLASSES: 
        return MESSAGE_CLASSES[type].decode(data[1:])

    raise DecodeError(f"Unknown MsgType: {type}")
