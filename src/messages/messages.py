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
        if len(data) < 6:
            raise DecodeError("Insufficient data to decode ClientData: header too short")
        _, dataset, data_length = struct.unpack('>BBI', data[:6])
        if len(data) < 6 + data_length:
            raise DecodeError(f"Insufficient data to decode ClientData: expected {6 + data_length}, got {len(data)}")
        rows_str = data[6:6 + data_length].decode()
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
        if len(data) < 7:
            raise DecodeError("Insufficient data to decode Data: header too short")
        _, id, dataset, data_length = struct.unpack('>BBBI', data[:7])
        if len(data) < 7 + data_length:
            raise DecodeError(f"Insufficient data to decode Data: expected {7 + data_length}, got {len(data)}")
        rows_str = data[7:7 + data_length].decode()
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
        if len(data) < 2:
            raise DecodeError("Insufficient data to decode Fin")
        _, id = struct.unpack('>BB', data[:2])
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
        if len(data) < 3:
            raise DecodeError("Insufficient data to decode CoordFin")
        _, id, node_id = struct.unpack('>BBB', data[:3])
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
        if len(data) < 5:
            raise DecodeError("Insufficient data to decode PushDataMessage: header too short")
        _, data_len = struct.unpack('>BI', data[:5])
        if len(data) < 5 + data_len:
            raise DecodeError(f"Insufficient data to decode PushDataMessage: expected {5 + data_len}, got {len(data)}")
        data_json = data[5:5 + data_len].decode()
        parsed_data = json.loads(data_json)
        return cls(data=parsed_data)

    def __str__(self):
        return f"PushDataMessage(data={self.data})"
    
# ===================================================================================================================== #

class ResultMessage(BaseMessage):

    RESULT_CLASSES = {
        1: Q1Result,
        2: Q2Result,
        3: Q3Result,
        4: Q4Result,
        5: Q5Result,
    }

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
    def decode(cls: Type[T], data: bytes) -> T:
        if len(data) < 4:
            raise DecodeError("Insufficient data to decode ResultMessage: header too short")
        
        _, result_type, id = struct.unpack('>BBB', data[:3])
        result_cls = cls.RESULT_CLASSES.get(result_type)
        
        if result_cls is None:
            raise DecodeError(f"Unknown result type: {result_type}")

        result = result_cls.decode(data[3:])
        return cls(id=id, result_type=QueryNumber(result_type), result=result)

# ========================================================================================================== #

class ListMessage(BaseMessage):

    # Mapeo de MsgType a los diccionarios que relacionan subtipos con clases
    TYPE_CLASSES = {
        MsgType.GAMES.value: {  # Mapeo para juegos
            0: BasicGame,
            1: Q1Game,
            2: Q2Game,
            3: GenreGame,
        },
        MsgType.REVIEWS.value: {  # Mapeo para reseñas
            0: Review,
            1: BasicReview,
            2: TextReview,
        },
    }

    def __init__(self, type: MsgType, item_type: Enum, items: List[T], id: int):
        """
        Mensaje genérico para listas de elementos.
        :param type: Tipo de mensaje (MsgType, como GAMES o REVIEWS).
        :param item_type: Subtipo de los elementos (GamesType, ReviewsType, etc.).
        :param items: Lista de elementos.
        :param id: Identificador único del mensaje.
        """
        super().__init__(type, items=items, item_type=item_type, id=id)
        self.item_type = item_type
        self.items = items
        self.id = id

    @staticmethod
    def decode_items(data: bytes, count: int, item_cls: Type[T]) -> List[T]:
        """
        Decodifica una lista de elementos desde bytes.
        :param data: Datos binarios.
        :param count: Número de elementos a decodificar.
        :param item_cls: Clase que define el tipo de elementos.
        :return: Lista de objetos decodificados.
        """
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

    @handle_encode_error
    def encode(self) -> bytes:
        """
        Codifica el mensaje en un formato binario.
        """
        items_bytes = b"".join([item.encode() for item in self.items])
        # Empaquetar el tipo de mensaje, tipo de elementos, identificador y cantidad de elementos
        body = struct.pack('>BBBH', int(self.type.value), self.item_type.value, self.id, len(self.items)) + items_bytes
        return body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """
        Decodifica un mensaje binario a un objeto `ListMessage`.
        :param data: Los datos binarios a decodificar.
        """
        if len(data) < 5:
            raise DecodeError("Insufficient data to decode ListMessage: header too short")
        
        type_value, item_type_value, id, items_count = struct.unpack('>BBBH', data[:5])

        # Determinar la clase de los elementos según el tipo y subtipo
        type_classes = cls.TYPE_CLASSES.get(type_value)

        item_cls = type_classes.get(item_type_value)

        # Decodificar los elementos
        items = cls.decode_items(data[5:], items_count, item_cls)
        return cls(type=MsgType(type_value), item_type=item_type_value, items=items, id=id)

    def __str__(self):
        return f"ListMessage(type={self.type}, item_type={self.item_type}, id={self.id}, items={self.items})"


# Uso General del Decode
MESSAGE_CLASSES = {
    MsgType.HANDSHAKE: Handshake,
    MsgType.FIN: Fin,
    MsgType.KEEP_ALIVE: KeepAlive,
    MsgType.ALIVE: Alive,
    MsgType.DATA: Data,
    MsgType.PUSH_DATA: PushDataMessage,
    MsgType.PULL_DATA: PullData,
    MsgType.GAMES: ListMessage,
    MsgType.REVIEWS: ListMessage,
    MsgType.RESULT: ResultMessage,
    MsgType.CLIENT_DATA: ClientData,
    MsgType.CLIENT_FIN: ClientFin,
    MsgType.COORDFIN: CoordFin,
}

def decode_msg(data: bytes):
    try:
        type = MsgType(data[0])
        return MESSAGE_CLASSES[type].decode(data)
    except IndexError:
        raise DecodeError("Data too short to determine message type")
    except ValueError:
        raise DecodeError(f"Unknown MsgType: {data[0]}")
