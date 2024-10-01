from enum import Enum
import struct
import logging

# Definición de los tipos de mensajes
MSG_TYPE_HANDSHAKE = 0x00
MSG_TYPE_DATA= 0x01
MSG_TYPE_FIN = 0x02

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
# Byte 3-4: Largo de los datos (2 bytes).
# Byte 5-N: Datos (cadena codificada).

# =========================================

def encode_handshake(id):
    """
    Codifica un mensaje handshake, incluyendo el largo total del mensaje.
    """
    # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
    body = struct.pack('>BB', MSG_TYPE_HANDSHAKE, id)
    
    # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
    total_length = len(body)
    
    # Empaquetamos el largo total seguido del cuerpo
    return struct.pack('>H', total_length) + body

def encode_fin(id):
    """
    Codifica un mensaje de tipo fin, incluyendo el largo total del mensaje.
    """
    # Empaquetamos el tipo de mensaje y el ID (1 byte cada uno)
    body = struct.pack('>BB', MSG_TYPE_FIN, id)
    
    # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
    total_length = len(body)
    
    # Empaquetamos el largo total seguido del cuerpo
    return struct.pack('>H', total_length) + body

def encode_data(id, data_str):
    """
    Codifica un mensaje de tipo data, incluyendo el largo total del mensaje.
    """
    # Convertimos los datos a bytes
    data_bytes = data_str.encode('utf-8')
    data_length = len(data_bytes)
    
    # Empaquetamos el tipo de mensaje, el ID, y la longitud de los datos (2 bytes)
    body = struct.pack('>BBH', MSG_TYPE_DATA, id, data_length) + data_bytes
    
    # Calcular la longitud total del mensaje (2 bytes de longitud + cuerpo)
    total_length = len(body)
    
    # Empaquetamos el largo total seguido del cuerpo
    return struct.pack('>H', total_length) + body

def decode_msg(data):
    """
    Decodifica un mensaje recibido en su estructura de datos correspondiente.
    """
    # El tipo de mensaje está en el 0
    tipo_mensaje = data[0]

    if tipo_mensaje == MSG_TYPE_HANDSHAKE:
        return decode_handshake(data[1:])
    elif tipo_mensaje == MSG_TYPE_DATA:
        return decode_data(data[1:])
    elif tipo_mensaje == MSG_TYPE_FIN:
        return decode_fin(data[1:])
    else:
        raise ValueError(f"Tipo de mensaje desconocido: {tipo_mensaje}")

def decode_handshake(data):
    id = struct.unpack('>B', data[:1])[0]
    return {'tipo': 'handshake', 'id': id}

def decode_fin(data):
    id = struct.unpack('>B', data[:1])[0]
    return {'tipo': 'fin', 'id': id}

def decode_data(data):
    id, data_length = struct.unpack('>BH', data[:3])
    data_str = data[3:3+data_length].decode('utf-8')
    return {'tipo': 'data', 'id': id, 'data': data_str}


class MsgType(Enum):
    HANDSHAKE = 0
    DATA = 1
    FIN = 2

class Handshake():
    def __init__(self, id: int):
        self.id = int(id)

    def encode(self) -> bytes:
        print("entre a encodear")
        msg_bytes = struct.pack("!B", MsgType.HANDSHAKE.value) + struct.pack("!B", self.id)
        print("empaqueté msg_bytes")
        msg_length = len(msg_bytes)
        retorno = struct.pack("!B", msg_length) + msg_bytes
        print(f"este es mi retorno {retorno}")
        return retorno
    
    def decode(data) -> 'Handshake':
        msg_id = struct.unpack("!B", data[:1])
        return Handshake(msg_id)

class Data():
    def __init__(self, id: int, row: str) -> None:
        self.id = id
        self.row = row
    
    def encode(self) -> bytes:
        # Codificamos el mensaje con el tipo DATA, el id, el largo de la información y la información
        message_bytes = self.row.encode('utf-8')
        length = len(message_bytes)
        msg_bytes = struct.pack("!B", MsgType.DATA.value) + struct.pack("!B", self.id) + struct.pack("!B", length) + message_bytes
        msg_length = len(msg_bytes)
        return struct.pack("!B", msg_length) + msg_bytes
    
    def decode(self, data) -> 'Data':
        # Decodificamos el tipo de mensaje, el id y el campo de información
        msg_id, length = struct.unpack("!BB", data[:2])
        message_bytes = data[2:2+length]
        row = message_bytes.decode('utf-8')
        return Data(msg_id, row)

class Fin():
    def __init__(self, id: int):
        self.id = id

    def encode(self) -> bytes:
        msg_bytes = struct.pack("!B", MsgType.FIN.value) + struct.pack("!B", self.id)
        msg_length = len(msg_bytes)
        return struct.pack("!B", msg_length) + msg_bytes

    def decode(data) -> 'Fin':
        msg_id = struct.unpack("!B", data[:1])
        return Fin(msg_id)