import struct

BYTES_HEADER = 4

def safe_read(sock, n_bytes: int):
    """Función que lee datos del socket, devolviendo mensajes completos uno por vez."""
    buffer = bytearray()
    try:
        while len(buffer) < n_bytes:
            chunk = sock.recv(n_bytes)
            if not chunk:
                raise Exception("No data received, client may have closed the connection.")
            buffer.extend(chunk)
        return buffer
    except OSError as e:  # Aquí cambiamos socket.error por OSError
        raise Exception(f"Error receiving data: {e}")

def _recv_all(sock, length):
    """
    Asegura la recepción de exactamente 'length' bytes desde el socket.
    """
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            return None  # Conexión cerrada o no se pudieron leer más datos
        data.extend(packet)
    return data

import struct

def recv_msg(sock):
    """
    Lee un mensaje completo del socket y devuelve la data, incluyendo
    4 bytes de longitud al principio para compatibilidad con `decode_msg`.
    """

    # Leer el encabezado de 4 bytes que indica la longitud del mensaje
    header = _recv_all(sock, BYTES_HEADER)
    if not header:
        raise ConnectionError("Conexión cerrada durante la lectura del encabezado del mensaje.")
    
    # Desempaquetar el encabezado para obtener la longitud total del mensaje
    total_length = struct.unpack('>I', header)[0]
    
    # Leer el resto del mensaje basado en la longitud especificada en el encabezado
    data = _recv_all(sock, total_length)
    
    if not data:
        raise ValueError("No se pudo leer el cuerpo del mensaje; posible desconexión.")
    
    return data

class DecodeError(Exception):
    """Excepción personalizada para errores de decodificación."""
    def __init__(self, message: str):
        super().__init__(f"DecodeError: {message}")


class EncodeError(Exception):
    """Excepción personalizada para errores de codificación."""
    def __init__(self, message: str):
        super().__init__(f"EncodeError: {message}")

def handle_encode_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise EncodeError(f"Error in {func.__name__}: {e}")
    return wrapper

