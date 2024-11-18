from enum import Enum
import struct
import json
from typing import List, Type, TypeVar

from messages.games_msg import BasicGame, Game, GamesType, GenreGame, Q1Game, Q2Game
from messages.results_msg import Q1Result, Q2Result, Q3Result, Q4Result, Q5Result, Result
from messages.reviews_msg import BasicReview, Review, ReviewsType, TextReview

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

# IMPORTANTE
# IMPORTANTE
# IMPORTANTE   En el encode se agrega el largo total del mensaje primero, en el decode ya no lo tiene
# IMPORTANTE
# IMPORTANTE

class BaseMessage:
    def __init__(self, msg_type: MsgType, **kwargs):
        self.msg_type = msg_type
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
    def encode(self) -> bytes:
        body = struct.pack('>B', self.msg_type.value)
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        return cls(msg_type=MsgType(struct.unpack('>B', data[0:1])[0]))
    
class Handshake(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.HANDSHAKE)

class KeepAlive(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.KEEP_ALIVE)

class Alive(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.ALIVE)

class PullData(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.PULL_DATA)

class ClientFin(SimpleMessage):
    def __init__(self):
        super().__init__(MsgType.CLIENT_FIN)

# ===================================================================================================================== #

class ClientData(BaseMessage):
    def __init__(self, rows: List[str], dataset: Dataset):
        super().__init__(MsgType.CLIENT_DATA, rows=rows, dataset=dataset)

    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)

        # Empaquetamos el tipo de mensaje, el dataset y la longitud de los datos
        body = struct.pack('>BBI', self.msg_type.value, self.dataset.value, data_length) + data_bytes

        # Calcular la longitud total del mensaje
        total_length = len(body)

        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        # Decodifica el dataset y la longitud de los datos
        dataset, data_length = struct.unpack('>BI', data[:5])

        # Decodifica el bloque completo de datos como un solo string y luego lo separa en filas
        rows_str = data[5:5 + data_length].decode()
        rows = rows_str.split("\n")

        # Crea y retorna una instancia de ClientData usando cls
        return cls(rows=rows, dataset=Dataset(dataset))

    def __str__(self):
        return f"ClientData(rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Data(BaseMessage):
    def __init__(self, id: int, rows: List[str], dataset: Dataset):
        super().__init__(MsgType.DATA, id=id, rows=rows, dataset=dataset)

    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el ID, el dataset y la longitud de los datos (4 bytes)
        body = struct.pack('>BBBI', MsgType.DATA.value, self.id, self.dataset.value, data_length) + data_bytes

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        # Decodifica el ID y el dataset
        id, dataset, data_lenght = struct.unpack('>BBI', data[:6])

        # Decodifica el bloque de datos como un solo string y luego lo separa en filas
        rows_str = data[6:6+data_lenght].decode()
        rows = rows_str.split("\n")

        # Crea y retorna una instancia de Data usando cls
        return cls(id=id, rows=rows, dataset=Dataset(dataset))

    def __str__(self):
        return f"Data(id={self.id}, rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Fin(BaseMessage):
    def __init__(self, id: int):
        super().__init__(MsgType.FIN, id=id)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID
        return struct.pack('>BB', self.msg_type.value, self.id)

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        # Decodifica el mensaje Fin
        id = struct.unpack('>B', data[:1])[0]
        return cls(id=id)

    def __str__(self):
        return f"Fin(id={self.id})"

# ===================================================================================================================== #
   
class CoordFin(BaseMessage):
    def __init__(self, id: int, node_id: int):
        super().__init__(MsgType.COORDFIN, id=id, node_id=node_id)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BBB', self.msg_type.value, self.id, self.node_id)
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        # Decodifica el mensaje Fin
        id, node_id = struct.unpack('>BB', data)
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

    def encode(self) -> bytes:
        """
        Codifica el mensaje en formato binario.
        """
        # Serializar el diccionario de datos a JSON
        data_json = json.dumps(self.data)
        data_bytes = data_json.encode()

        # Empaquetar encabezado: tipo de mensaje y longitud de los datos
        body = struct.pack(
            '>BI', self.msg_type.value, len(data_bytes)
        ) + data_bytes

        # Calcular la longitud total del mensaje
        total_length = len(body)

        # Empaquetar el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """
        Decodifica un mensaje `PushDataMessage` desde formato binario.
        """
        # Leer encabezado
        data_len = struct.unpack('>I', data[:4])[0]

        # Extraer y decodificar los datos JSON
        data_json = data[4:4 + data_len].decode()
        parsed_data = json.loads(data_json)

        return cls(data=parsed_data)

    def __str__(self):
        return f"PushDataMessage(data={self.data})"
    
# ===================================================================================================================== #
    
class GamesMessage(BaseMessage):
    def __init__(self, msg_type: MsgType, game_type: GamesType, games: List[Game], id: int):
        super().__init__(msg_type, games=games, game_type=game_type, id=id)

    def encode(self) -> bytes:
        games_bytes = b"".join([game.encode() for game in self.games])
        # Empaquetar el tipo de mensaje, el tipo de juego, y la cantidad de juegos
        body = struct.pack('>BBBH', self.msg_type.value, self.game_type.value, self.id, len(self.games)) + games_bytes
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes, game_cls: Type[Game]) -> T:
        id, games_count = struct.unpack('>BH', data[:3])
        offset = 3
        games = []

        for _ in range(games_count):
            game_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            game_data = data[offset:offset + game_length]
            offset += game_length
            games.append(game_cls.decode(game_data))

        return cls(MsgType.GAMES, GamesType(game_cls), games, id=id)

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
    def __init__(self, msg_type: MsgType, review_type: ReviewsType, reviews: List[Review], id: int):
        super().__init__(msg_type, reviews=reviews, review_type=review_type, id=id)

    def encode(self) -> bytes:
        reviews_bytes = b"".join([review.encode() for review in self.reviews])
        # Empaquetar el tipo de mensaje, el tipo de reseña, y la cantidad de reseñas
        body = struct.pack('>BBBH', self.msg_type.value, self.review_type.value, self.id, len(self.reviews)) + reviews_bytes
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes, review_cls: Type[Review]) -> T:
        id, reviews_count = struct.unpack('>BH', data[:3])
        offset = 3
        reviews = []

        for _ in range(reviews_count):
            review_length = struct.unpack('>I', data[offset:offset + 4])[0]
            offset += 4
            review_data = data[offset:offset + review_length]
            offset += review_length
            reviews.append(review_cls.decode(review_data))

        return cls(MsgType.REVIEWS, ReviewsType(review_cls), reviews, id=id)

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
    def __init__(self, id: int, result: Result):
        super().__init__(MsgType.RESULT, id=id, result=result)

    def encode(self) -> bytes:
        # Codifica el tipo de mensaje seguido del tipo de resultado y el resultado específico
        body = struct.pack('>BBB', self.msg_type.value, self.result.result_type.value, self.id) + self.result.encode()
        # Empaqueta la longitud del mensaje
        total_length = len(body)
        return struct.pack('>I', total_length) + body

    @classmethod
    def decode(cls: Type[T], data: bytes, result_cls: Type[Result]) -> T:
        # Decodifica el resultado utilizando la clase específica adecuada
        id = struct.unpack('>B', data[:1])[0]
        offset = 1
        result = result_cls.decode(data[offset:])
        return cls(MsgType.RESULT, id, result=result)
    

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
    msg_type = MsgType(data[0])

    if msg_type in [MsgType.GAMES, MsgType.REVIEWS, MsgType.RESULT]:
        cls_map = {
            MsgType.GAMES: GAME_CLASSES,
            MsgType.REVIEWS: REVIEW_CLASSES,
            MsgType.RESULT: RESULT_CLASSES,
        }
        sub_cls_map = cls_map[msg_type]
        sub_type = data[1]
        sub_cls = sub_cls_map[sub_type]

        if msg_type == MsgType.RESULT:
            return ResultMessage(MESSAGE_CLASSES[msg_type].decode(data[2:], sub_cls))
        
        return MESSAGE_CLASSES[msg_type].decode(data[2:], sub_cls)

    elif msg_type in MESSAGE_CLASSES: 
        return MESSAGE_CLASSES[msg_type].decode(data[1:])

    raise ValueError(f"Tipo de mensaje desconocido: {msg_type}")