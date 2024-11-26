from enum import Enum
import struct
import json
from typing import List, Type, TypeVar

from messages.games_msg import BasicGame, GamesType, GenreGame, Q1Game, Q2Game
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
    ELECTION = 13
    OK_ELECTION = 14
    LEADER_ELECTION = 15
    COORDFIN_ACK = 16

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
    def __init__(self, type: MsgType = 0, msg_id: int = 0, **kwargs):
        """
        Clase base para todos los mensajes.

        :param type: Tipo de mensaje (MsgType).
        :param msg_id: Identificador único del mensaje (entero de 4 bytes).
        """
        self.type = type
        self.msg_id = msg_id  # Agregar msg_id como atributo
        for key, value in kwargs.items():
            setattr(self, key, value)

    def encode(self) -> bytes:
        """Codifica el mensaje en binario.

        Este método debe ser implementado en subclases para definir cómo
        se serializa el mensaje en formato binario.

        :raises NotImplementedError: Si no está implementado en una subclase.
        """
        raise NotImplementedError("Debe implementarse en subclases")

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """Decodifica un mensaje desde binario.

        Este método debe ser implementado en subclases para definir cómo
        se deserializa un mensaje desde formato binario.

        :param data: Los datos binarios que representan el mensaje.
        :raises NotImplementedError: Si no está implementado en una subclase.
        """
        raise NotImplementedError("Debe implementarse en subclases")
    
    def base_encode(self) -> bytes:
        """Codifica el `type` y `msg_id` comunes a todos los mensajes.

        Orden: `type` (1 byte) seguido de `msg_id` (4 bytes).

        :return: Los datos binarios que incluyen `type` y `msg_id`.
        """
        return struct.pack('>BI', self.type.value, self.msg_id)

    @classmethod
    def base_decode(cls: Type[T], data: bytes) -> (MsgType, int, bytes): # type: ignore
        """Decodifica el `type` y `msg_id` comunes a todos los mensajes.

        Orden: `type` (1 byte) seguido de `msg_id` (4 bytes).

        :param data: Los datos binarios del mensaje.
        :return: Una tupla con `type`, `msg_id` y el resto de los datos.
        :raises DecodeError: Si los datos son insuficientes.
        """
        if len(data) < 5:  # 1 byte para `type` y 4 bytes para `msg_id`
            raise DecodeError("Insufficient data to decode `type` and `msg_id`")

        # Decodificar el `type` y el `msg_id`
        type_value, msg_id = struct.unpack('>BI', data[:5])
        remaining_data = data[5:]

        try:
            msg_type = MsgType(type_value)
        except ValueError:
            raise DecodeError(f"Unknown MsgType: {type_value}")

        return msg_type, msg_id, remaining_data
    
    def add_msg_len(self, body: bytes) -> bytes:
        """Agrega la longitud total del mensaje al inicio del cuerpo.

        Esto es útil para protocolos de comunicación que requieren
        que el tamaño del mensaje esté incluido al comienzo.

        :param body: Los datos binarios del mensaje.
        :return: Los datos binarios con el tamaño total prepended.
        """
        total_len = len(body)
        return struct.pack('>I', total_len) + body

    def __str__(self):
        return f"{self.__class__.__name__}({vars(self)})"
    
# ===================================================================================================================== #

""" MENSAJE SIMPLE (atributos de un solo byte) CON FLAG PARA INDICAR SI ES PARA SOCKET O NO (incluye el largo o no del body)"""
class SimpleMessage(BaseMessage):
    def __init__(self, type: MsgType, socket_compatible: bool = False, msg_id: int = 0, **kwargs):
        """
        Mensaje simple con campos adicionales.

        :param type: Tipo de mensaje (MsgType).
        :param msg_id: Identificador único del mensaje.
        :param socket_compatible: Flag para incluir el tamaño total en la codificación.
        """
        super().__init__(type=type, msg_id=msg_id, **kwargs)
        self.socket_compatible = socket_compatible

    @handle_encode_error
    def encode(self) -> bytes:
        """Codifica un mensaje simple en formato binario.

        Si `socket_compatible` es True, se agrega la longitud total del
        mensaje al inicio del cuerpo. Los atributos adicionales se codifican
        como enteros de un byte.

        :return: El mensaje codificado en binario.
        """
        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Codificar atributos específicos
        body = b""
        for attr, value in vars(self).items():
            if attr not in {"type", "msg_id", "socket_compatible"}:
                body += struct.pack('>B', value)

        # Concatenar la base y los datos específicos
        encoded_message = base_data + body

        # Agregar largo total si es necesario
        if self.socket_compatible:
            encoded_message = self.add_msg_len(encoded_message)

        return encoded_message

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """Decodifica un mensaje simple desde formato binario.

        Determina los atributos adicionales según el tipo de mensaje.

        :param data: Los datos binarios del mensaje.
        :return: Una instancia de `SimpleMessage`.
        :raises DecodeError: Si los datos son insuficientes o no coinciden con
                            el tipo esperado.
        """
        ATTRIBUTE_MAPPING = {
            MsgType.FIN: ["client_id"],
            MsgType.ELECTION: ["client_id"],
            MsgType.OK_ELECTION: ["client_id"],
            MsgType.LEADER_ELECTION: ["client_id"],
            MsgType.COORDFIN: ["client_id", "node_id"],
            MsgType.COORDFIN_ACK: ["client_id"]
        }

        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)

        # Determinar el mapeo de atributos según el tipo de mensaje
        attribute_names = ATTRIBUTE_MAPPING.get(msg_type, [])

        if len(remaining_data) < len(attribute_names):
            raise DecodeError(f"Insufficient data to decode attributes for {msg_type}")
        
        # Decodificar los campos adicionales en base al número esperado de atributos
        format_string = f'>{len(attribute_names)}B'
        fields = struct.unpack(format_string, remaining_data[:len(attribute_names)])

        if len(fields) != len(attribute_names):
            raise DecodeError(f"Expected {len(attribute_names)} fields for {type}, got {len(fields)}")

        # Crear un diccionario de atributos con nombres correctos
        attributes = dict(zip(attribute_names, fields))

        # Instanciar la clase con los atributos
        return cls(type=msg_type, msg_id=msg_id, **attributes)

# ===================================================================================================================== #

class ClientData(BaseMessage):
    def __init__(self, rows: List[str], dataset: Dataset, msg_id: int = 0):
        """
        Mensaje que contiene datos del cliente.

        :param msg_id: Identificador único del mensaje.
        :param rows: Lista de filas de datos.
        :param dataset: Tipo de dataset (Dataset).
        """
        super().__init__(MsgType.CLIENT_DATA, msg_id=msg_id, rows=rows, dataset=dataset)

    @handle_encode_error
    def encode(self) -> bytes:
        """Codifica un mensaje `ClientData` en binario.

        Incluye el `msg_id`, el tipo de mensaje (`type`), el dataset y las filas (`rows`).
        """

        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Codificar atributos específicos
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)

        # Codificar dataset, longitud de datos y datos
        body = struct.pack('>BI', self.dataset.value, data_length) + data_bytes

        # añadir longitud total al mensaje y retornar
        body_with_len = self.add_msg_len(base_data + body)
        return body_with_len

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """Decodifica un mensaje `ClientData` desde binario.

        Extrae el `msg_id`, el dataset y las filas (`rows`).

        :param data: Los datos binarios del mensaje.
        :return: Una instancia de `ClientData`.
        :raises DecodeError: Si los datos son insuficientes o inválidos.
        """

        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)

        if msg_type != MsgType.CLIENT_DATA:
            raise DecodeError(f"Invalid message type: expected {MsgType.CLIENT_DATA}, got {msg_type}")
        

        # Decodificar atributos específicos
        if len(remaining_data) < 5:  # 1 byte para dataset y 4 bytes para data_length
            raise DecodeError("Insufficient data to decode ClientData header")

        dataset_value, data_length = struct.unpack('>BI', remaining_data[:5])
        if len(remaining_data) < 5 + data_length:
            raise DecodeError(f"Insufficient data to decode ClientData: expected {5 + data_length}, got {len(remaining_data)}")

        # Decodificar filas
        rows_data = remaining_data[5:5 + data_length].decode()
        rows = rows_data.split("\n")
        return cls(rows=rows, dataset=Dataset(dataset_value), msg_id=msg_id)

    def __str__(self):
        """Representación legible del mensaje."""
        return f"ClientData(msg_id={self.msg_id}, rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

class Data(BaseMessage):
    def __init__(self, client_id: int, rows: List[str], dataset: Dataset, msg_id: int = 0):
        """
        Mensaje de datos con información del cliente.

        :param client_id: Identificador único del cliente.
        :param rows: Lista de filas de datos.
        :param dataset: Tipo de dataset (Dataset).
        :param msg_id: Identificador único del mensaje.
        """
        super().__init__(MsgType.DATA, msg_id=msg_id, client_id=client_id, rows=rows, dataset=dataset)

    @handle_encode_error
    def encode(self) -> bytes:
        """Codifica un mensaje `Data` en binario.

        Incluye el `msg_id`, el tipo de mensaje (`type`), el `client_id`,
        el dataset y las filas (`rows`).
        """
        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Codificar atributos específicos
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)
        
        # Codificar `client_id`, `dataset` y longitud de datos
        body = struct.pack('>BBI', self.client_id, self.dataset.value, data_length) + data_bytes

        # Concatenar la base y los datos específicos
        return base_data + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """Decodifica un mensaje `Data` desde binario.

        Extrae el `msg_id`, el `client_id`, el dataset y las filas (`rows`).

        :param data: Los datos binarios del mensaje.
        :return: Una instancia de `Data`.
        :raises DecodeError: Si los datos son insuficientes o inválidos.
        """
        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)

        if msg_type != MsgType.DATA:
            raise DecodeError(f"Invalid message type: expected {MsgType.DATA}, got {msg_type}")

        # Decodificar atributos específicos
        if len(remaining_data) < 6:  # 1 byte para `client_id`, 1 byte para `dataset`, 4 bytes para `data_length`
            raise DecodeError("Insufficient data to decode Data header")

        client_id, dataset_value, data_length = struct.unpack('>BBI', remaining_data[:6])

        if len(remaining_data) < 6 + data_length:
            raise DecodeError(f"Insufficient data to decode Data: expected {6 + data_length}, got {len(remaining_data)}")

        # Decodificar filas
        rows_data = remaining_data[6:6 + data_length].decode()
        rows = rows_data.split("\n")

        return cls(client_id=client_id, rows=rows, dataset=Dataset(dataset_value), msg_id=msg_id)

    def __str__(self):
        """Representación legible del mensaje."""
        return f"Data(msg_id={self.msg_id}, client_id={self.client_id}, rows={self.rows}, dataset={self.dataset})"

# ===================================================================================================================== #

def convert_keys_to_int(obj):
    """
    Convierte las claves que son cadenas numéricas a enteros en un diccionario anidado.
    """
    if isinstance(obj, dict):
        return {
            int(k) if k.isdigit() else k: convert_keys_to_int(v)
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [convert_keys_to_int(item) for item in obj]
    return obj
    
import json

class PushDataMessage(BaseMessage):
    def __init__(self, data: dict, msg_id: int = 0):
        """
        Mensaje genérico para enviar datos arbitrarios entre nodos y réplicas.

        :param msg_id: Identificador único del mensaje.
        :param data: Diccionario con los datos a enviar.
        """
        super().__init__(MsgType.PUSH_DATA, msg_id=msg_id, data=data)

    @handle_encode_error
    def encode(self) -> bytes:
        """
        Codifica el mensaje en formato binario.

        Incluye el msg_id, tipo de mensaje (`type`) y los datos serializados.
        """
        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Serializar el diccionario de datos a JSON
        data_json = json.dumps(self.data)
        data_bytes = data_json.encode()

        # Codificar la longitud del mensaje y los datos
        body = struct.pack('>I', len(data_bytes)) + data_bytes

        # Concatenar la base y el cuerpo
        return base_data + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """
        Decodifica un mensaje `PushDataMessage` desde binario.

        Extrae el msg_id, tipo de mensaje y los datos serializados.
        """
        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)

        if msg_type != MsgType.PUSH_DATA:
            raise DecodeError(f"Invalid message type: expected {MsgType.PUSH_DATA}, got {msg_type}")
        
        # Decodificar la longitud de los datos
        if len(remaining_data) < 4:
            raise DecodeError("Insufficient data to decode PushDataMessage length")
        data_len = struct.unpack('>I', remaining_data[:4])[0]

        if len(remaining_data) < 4 + data_len:
            raise DecodeError(f"Insufficient data to decode PushDataMessage: expected {4 + data_len}, got {len(remaining_data)}")

        # Decodificar los datos en formato JSON
        data_json = remaining_data[4:4 + data_len].decode()
        parsed_data = json.loads(data_json)

        parsed_data = convert_keys_to_int(parsed_data)
        return cls(data=parsed_data, msg_id=msg_id)

    def __str__(self):
        """
        Representación legible del mensaje.
        """
        return f"PushDataMessage(msg_id={self.msg_id}, data={self.data})"
    
# ===================================================================================================================== #

class ResultMessage(BaseMessage):

    RESULT_CLASSES = {
        1: Q1Result,
        2: Q2Result,
        3: Q3Result,
        4: Q4Result,
        5: Q5Result,
    }

    def __init__(self, client_id: int, result_type: QueryNumber, result: Result, msg_id: int = 0):
        """
        Mensaje que encapsula un resultado.

        :param client_id: Identificador único del cliente.
        :param result_type: Tipo de resultado (QueryNumber).
        :param result: Objeto del resultado correspondiente.
        :param msg_id: Identificador único del mensaje.
        """
        super().__init__(MsgType.RESULT, msg_id=msg_id, client_id=client_id, result_type=result_type, result=result)

    @handle_encode_error
    def encode(self) -> bytes:
        """
        Codifica un mensaje `ResultMessage` en binario.

        Incluye el msg_id, tipo de mensaje (`type`), client_id, tipo de resultado
        y los datos específicos del resultado.
        """
        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Codificar atributos específicos
        body = struct.pack('>BB', int(self.result_type.value), self.client_id) + self.result.encode()
        body_with_len = self.add_msg_len(base_data + body)
        return body_with_len

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """
        Decodifica un mensaje `ResultMessage` desde binario.

        Extrae el msg_id, tipo de mensaje, client_id, tipo de resultado
        y los datos específicos del resultado.

        :param data: Datos binarios a decodificar.
        :return: Instancia de `ResultMessage`.
        :raises DecodeError: Si los datos son insuficientes o inválidos.
        """

        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)

        if msg_type != MsgType.RESULT:
            raise DecodeError(f"Invalid message type: expected {MsgType.RESULT}, got {msg_type}")
        
        # Decodificar el tipo de resultado y el client_id
        if len(remaining_data) < 2:
            raise DecodeError("Insufficient data to decode ResultMessage header")
        result_type_value, client_id = struct.unpack('>BB', remaining_data[:2])
        
        # Determinar la clase del resultado
        result_cls = cls.RESULT_CLASSES.get(result_type_value)
        if result_cls is None:
            raise DecodeError(f"Unknown result type: {result_type_value}")
        

        # Decodificar los datos específicos del resultado
        result = result_cls.decode(remaining_data[2:])

        return cls(
            client_id=client_id,
            result_type=QueryNumber(result_type_value),
            result=result,
            msg_id=msg_id
        )
    
    def __str__(self):
        """
        Representación legible del mensaje.
        """
        return f"ResultMessage(msg_id={self.msg_id}, client_id={self.client_id}, result_type={self.result_type}, result={self.result})"

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

    def __init__(self, type: MsgType, item_type: Enum, items: List[T], client_id: int, msg_id: int = 0):
        """
        Mensaje genérico para listas de elementos.
        :param msg_id: Identificador único del mensaje.
        :param type: Tipo de mensaje (MsgType, como GAMES o REVIEWS).
        :param item_type: Subtipo de los elementos (GamesType, ReviewsType, etc.).
        :param items: Lista de elementos.
        :param client_id: Id del cliente
        """
        super().__init__(type, msg_id=msg_id, items=items, item_type=item_type, client_id=client_id)

    @staticmethod
    def get_item_class(type_value: int, item_type_value: int):
        """
        Devuelve la clase asociada según el tipo y subtipo del mensaje.
        
        :param type_value: Valor del tipo de mensaje (MsgType).
        :param item_type_value: Valor del subtipo del mensaje (ReviewsType o GamesType).
        :return: Clase asociada al tipo y subtipo del mensaje.
        :raises DecodeError: Si el tipo o subtipo no es válido.
        """
        if type_value == MsgType.GAMES.value:
            item_cls = GamesType.get_class(item_type_value)
        elif type_value == MsgType.REVIEWS.value:
            item_cls = ReviewsType.get_class(item_type_value)
        else:
            raise DecodeError(f"Unknown type: {type_value}")
        
        if not item_cls:
            raise DecodeError(f"Unknown item type: {item_type_value}")
        
        return item_cls

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

        # Codificar los campos comunes (`type` y `msg_id`)
        base_data = self.base_encode()

        # Codificar atributos específicos
        items_bytes = b"".join([item.encode() for item in self.items])
        body = struct.pack('>BBH', self.item_type.value, self.client_id, len(self.items)) + items_bytes

        return base_data + body

    @classmethod
    def decode(cls: Type[T], data: bytes) -> T:
        """
        Decodifica un mensaje binario a un objeto `ListMessage`.

        :param data: Los datos binarios a decodificar.
        :return: Instancia de `ListMessage`.
        :raises DecodeError: Si los datos son insuficientes o inválidos.
        """
        # Decodificar los campos comunes (`type` y `msg_id`)
        msg_type, msg_id, remaining_data = cls.base_decode(data)
        if len(remaining_data) < 4:
            raise DecodeError("Insufficient data to decode ListMessage header")

        # Decodificar el item_type, client_id y cantidad de elementos
        item_type_value, client_id, items_count = struct.unpack('>BBH', remaining_data[:4])
        remaining_data = remaining_data[4:]

        # Obtener la clase del ítem
        item_cls = cls.get_item_class(msg_type.value, item_type_value)

        # Decodificar los elementos
        items = cls.decode_items(remaining_data, items_count, item_cls)

        return cls(
            type=MsgType(msg_type.value),
            item_type=item_type_value,
            items=items,
            client_id=client_id,
            msg_id=msg_id
        )

    def __str__(self):
        """
        Representación legible del mensaje.
        """
        return f"ListMessage(type={self.type}, msg_id={self.msg_id}, item_type={self.item_type}, client_id={self.client_id}, items={self.items})"


# Uso General del Decode
MESSAGE_CLASSES = {
    MsgType.GAMES: ListMessage,
    MsgType.REVIEWS: ListMessage,
    MsgType.RESULT: ResultMessage,
    MsgType.CLIENT_DATA: ClientData,
    MsgType.DATA: Data,
    MsgType.PUSH_DATA: PushDataMessage,
    #========== SimpleMessages ==========#
    MsgType.HANDSHAKE: SimpleMessage,
    MsgType.FIN: SimpleMessage,
    MsgType.PULL_DATA: SimpleMessage,
    MsgType.CLIENT_FIN: SimpleMessage,
    MsgType.ELECTION: SimpleMessage,
    MsgType.OK_ELECTION: SimpleMessage,
    MsgType.LEADER_ELECTION: SimpleMessage,
    MsgType.COORDFIN: SimpleMessage,
    MsgType.COORDFIN_ACK: SimpleMessage
}


def decode_msg(data: bytes):
    try:
        type = MsgType(data[0])
        msg_class = MESSAGE_CLASSES.get(type)
        if msg_class:
            return msg_class.decode(data)
        raise DecodeError(f"Unhandled MsgType: {data[0]}")
    except IndexError:
        raise DecodeError("Data too short to determine message type")
    except ValueError:
        raise DecodeError(f"Unknown MsgType: {data[0]}")


