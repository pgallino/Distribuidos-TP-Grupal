from enum import Enum
import struct
import logging

# Definición de los tipos de mensajes
MSG_TYPE_HANDSHAKE = 0x00
MSG_TYPE_DATA= 0x01
MSG_TYPE_FIN = 0x02

GAME_CSV = 0x00
REVIEW_CSV = 0x01

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
    tipo_mensaje = data[0]

    if tipo_mensaje == MSG_TYPE_HANDSHAKE:
        return Handshake.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif tipo_mensaje == MSG_TYPE_DATA:
        return Data.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    elif tipo_mensaje == MSG_TYPE_FIN:
        return Fin.decode(data[1:])  # Saltamos el primer byte (tipo de mensaje)

    else:
        raise ValueError(f"Tipo de mensaje desconocido: {tipo_mensaje}")

class Message:
    def __init__(self, id: int, type: int):
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
        super().__init__(id, MSG_TYPE_HANDSHAKE)

    def encode(self) -> bytes:
        # Codifica el mensaje Handshake
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', MSG_TYPE_HANDSHAKE, self.id)
        
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
    def __init__(self, id: int, row: str, dataset: int):
        super().__init__(id, MSG_TYPE_DATA)
        self.row = row
        self.dataset = dataset
    
    def encode(self) -> bytes:
        # Codifica el mensaje Data
        # Convertimos los datos a bytes
        data_bytes = self.row.encode('utf-8')
        data_length = len(data_bytes)
        
        # Empaquetamos el tipo de mensaje, el ID, el dataset y la longitud de los datos (2 bytes)
        body = struct.pack('>BBBH', MSG_TYPE_DATA, self.id, self.dataset, data_length) + data_bytes
        
        # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
        total_length = len(body)
        
        # Empaquetamos el largo total seguido del cuerpo
        return struct.pack('>H', total_length) + body
    
    @staticmethod
    def decode(data: bytes) -> 'Data':
        # Decodifica el mensaje Data
        id, dataset, data_length = struct.unpack('>BBH', data[:4])
        data_str = data[4:4+data_length].decode('utf-8')
        return Data(id, data_str, dataset)

    def __str__(self):
        return f"Data(id={self.id}, row='{self.row}', dataset={self.dataset})"

class Fin(Message):
    def __init__(self, id: int):
        super().__init__(id, MSG_TYPE_FIN)

    def encode(self) -> bytes:
        # Codifica el mensaje Fin
        # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
        body = struct.pack('>BB', MSG_TYPE_FIN, self.id)
        
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