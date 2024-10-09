import logging
import struct
import socket

BYTES_HEADER = 4

def safe_read(sock, n_bytes: int):
    """Función que lee datos del socket, devolviendo mensajes completos uno por vez."""
    buffer = bytearray()
    try:
        while len(buffer) < n_bytes:
            chunk = sock.recv(n_bytes)
            if not chunk:
                logging.info("No data received, client may have closed the connection.")
                return None
            buffer.extend(chunk)
        return buffer
    except OSError as e:  # Aquí cambiamos socket.error por OSError
        logging.error(f"Error receiving data: {e}")
        return None

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

def recv_msg(sock):
    """
    Lee un mensaje completo del socket y devuelve la data, incluyendo
    4 bytes de longitud al principio para compatibilidad con `decode_msg`.
    """
    # Leer el encabezado de 4 bytes que indica la longitud del mensaje
    header = _recv_all(sock, BYTES_HEADER)
    if not header:
        raise ValueError("Conexión cerrada o no se pudo leer el encabezado del mensaje.")
    
    # Desempaquetar el encabezado para obtener la longitud total del mensaje
    total_length = struct.unpack('>I', header)[0]
    
    # Leer el resto del mensaje basado en la longitud especificada en el encabezado
    data = _recv_all(sock, total_length)
    
    if not data:
        raise ValueError("No se pudo leer los datos del mensaje.")
    
    # Crear `raw_msg` con los 4 bytes del encabezado seguidos de los datos
    raw_msg = header + data
    return raw_msg
