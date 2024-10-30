from enum import Enum
import struct
from typing import List

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

# IMPORTANTE
# IMPORTANTE
# IMPORTANTE   En el encode se agrega el largo total del mensaje primero, en el decode ya no lo tiene
# IMPORTANTE
# IMPORTANTE

# Explicación wrappers:
# Primera Llamada: La primera vez que se llama a decode_XXX_wrapper, la función importa decode_XXX desde XXX_msg y lo almacena en _XXX_decoder.
# Llamadas Subsecuentes: En llamadas futuras, _XXX_decoder ya está asignado, así que se usa directamente sin volver a importar.
# Este método evita ciclos de importación y también minimiza el costo de importaciones repetitivas, ya que el módulo solo se importa la primera vez.

_game_decoder = None
_result_decoder = None
_review_decoder = None

def decode_game_wrapper(data):
    global _game_decoder
    if _game_decoder is None:
        from messages.games_msg import decode_game
        _game_decoder = decode_game
    return _game_decoder(data)

def decode_result_wrapper(data):
    global _result_decoder
    if _result_decoder is None:
        from messages.results_msg import decode_result
        _result_decoder = decode_result
    return _result_decoder(data)

def decode_review_wrapper(data):
    global _review_decoder
    if _review_decoder is None:
        from messages.reviews_msg import decode_review
        _review_decoder = decode_review
    return _review_decoder(data)


def decode_msg(data):
    """
    Decodifica un mensaje recibido y devuelve una instancia de la clase correspondiente.
    """

    msg_type = MsgType(data[0])

    # Saltamos el primer byte (tipo de mensaje)

    if msg_type == MsgType.HANDSHAKE:

        return Handshake.decode()

    elif msg_type == MsgType.DATA:
        return Data.decode(data[1:])

    elif msg_type == MsgType.FIN:
        return Fin.decode(data[1:])
    
    elif msg_type == MsgType.GAMES:
        
        return decode_game_wrapper(data[1:])
    
    elif msg_type == MsgType.REVIEWS:

        return decode_review_wrapper(data[1:])

    elif msg_type == MsgType.RESULT:

        return decode_result_wrapper(data[1:])
    
    elif msg_type == MsgType.COORDFIN:

        return CoordFin.decode(data[1:])
    
    elif msg_type == MsgType.CLIENT_FIN:

        return ClientFin.decode()
    
    elif msg_type == MsgType.CLIENT_DATA:

        return ClientData.decode(data[1:])

    else:
        raise ValueError(f"Tipo de mensaje desconocido: {msg_type}")

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
    def __init__(self):
        super().__init__(0, MsgType.HANDSHAKE)

    def encode(self) -> bytes:
        # Codifica el mensaje Handshake
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>B', int(MsgType.HANDSHAKE.value))
        
        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body
    
    @staticmethod
    def decode() -> 'Handshake':
        # Decodifica el mensaje Handshake
        return Handshake()
    
    def __str__(self):
        return f"Handshake()"

# ===================================================================================================================== #

class ClientData(Message):
    def __init__(self, rows: List[str], dataset: Dataset):
        super().__init__(0, MsgType.CLIENT_DATA)
        self.rows = rows  # Ahora se espera una lista de filas en lugar de una sola fila
        self.dataset = dataset
    
    def encode(self) -> bytes:
        # Convertimos cada fila a bytes y las unimos con un delimitador (por ejemplo, '\n')
        data_bytes = "\n".join(self.rows).encode()
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el dataset y la longitud de los datos (4 bytes)
        body = struct.pack('>BBI', int(MsgType.CLIENT_DATA.value), self.dataset.value, data_length) + data_bytes
        
        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode(data: bytes) -> 'ClientData':
        # Decodifica el mensaje Data
        dataset, data_length = struct.unpack('>BI', data[:5])
        
        # Decodifica el bloque completo de datos como un solo string y luego lo separa en filas
        rows_str = data[5:5+data_length].decode()
        rows = rows_str.split("\n")
        
        return ClientData(rows, Dataset(dataset))

    def __str__(self):
        return f"ClientData(rows={self.rows}, dataset={self.dataset})"

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
        
        return body

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

class Fin(Message):
    def __init__(self, id: int):
        super().__init__(id, MsgType.FIN)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', int(MsgType.FIN.value), self.id)
        
        # Empaquetamos el largo total seguido del cuerpo
        return body

    @staticmethod
    def decode(data: bytes) -> 'Fin':
        # Decodifica el mensaje Fin
        id = struct.unpack('>B', data[:1])[0]
        return Fin(id)

    def __str__(self):
        return f"Fin(id={self.id})"
    
class ClientFin(Message):
    def __init__(self):
        super().__init__(0, MsgType.CLIENT_FIN)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>B', int(MsgType.CLIENT_FIN.value))

        # Calcular la longitud total del mensaje (4 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>I', total_length) + body

    @staticmethod
    def decode() -> 'ClientFin':
        # Decodifica el mensaje Fin
        return ClientFin()

    def __str__(self):
        return f"Fin()"
    
class CoordFin(Message):
    def __init__(self, id: int, node_id: int):
        super().__init__(id, MsgType.COORDFIN)
        self.node_id = node_id

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BBB', int(MsgType.COORDFIN.value), self.id, self.node_id)
        
        return body

    @staticmethod
    def decode(data: bytes) -> 'CoordFin':
        # Decodifica el mensaje Fin
        id, node_id = struct.unpack('>BB', data)
        return CoordFin(id, node_id)

    def __str__(self):
        return f"CoordFin(id={self.id}, node_id={self.node_id})"