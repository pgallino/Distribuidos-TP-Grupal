from enum import Enum
import logging
import struct
import subprocess
import time

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

def reanimate_container(container_name, sleep_seconds=5):
    """Reanima un contenedor Docker específico.
    
    Verifica si el contenedor está activo, lo detiene si es necesario y luego lo reinicia.
    
    :param container_name: Nombre del contenedor a reiniciar.
    :param sleep_seconds: Tiempo en segundos entre detener y reiniciar el contenedor.
    """
    try:
        # Verificar si el contenedor está corriendo
        ps_result = subprocess.run(
            ['docker', 'ps', '--filter', f'name={container_name}', '--format', '{{.Names}}'],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        running_containers = ps_result.stdout.decode().strip().splitlines()

        logging.info(
            'Docker ps executed. Result: {}. Output: {}. Error: {}'.format(
                ps_result.returncode,
                ps_result.stdout.decode().strip(),
                ps_result.stderr.decode().strip()
            )
        )

        if container_name in running_containers:
            logging.info(f"Container {container_name} is running. Attempting to stop it.")
            
            # Detener el contenedor
            stop_result = subprocess.run(
                ['docker', 'stop', container_name],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logging.info(
                'Docker stop executed. Result: {}. Output: {}. Error: {}'.format(
                    stop_result.returncode,
                    stop_result.stdout.decode().strip(),
                    stop_result.stderr.decode().strip()
                )
            )

            if stop_result.returncode != 0:
                logging.error(f"Failed to stop container {container_name}. Aborting reanimation.")
                return
        else:
            logging.info(f"Container {container_name} is not running. No need to stop.")

        # Esperar antes de intentar reiniciar
        time.sleep(sleep_seconds)

        # Intentar reiniciar el contenedor
        start_result = subprocess.run(
            ['docker', 'start', container_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        logging.info(
            'Docker start executed. Result: {}. Output: {}. Error: {}'.format(
                start_result.returncode,
                start_result.stdout.decode().strip(),
                start_result.stderr.decode().strip()
            )
        )

        if start_result.returncode != 0:
            logging.error(f"Failed to start container {container_name}.")
        else:
            logging.info(f"Container {container_name} reanimated successfully.")

    except Exception as e:
        logging.error(f"Unexpected error while handling container {container_name}: {e}")

class TaskType(Enum):
    PULL = 0
    REANIMATE_MASTER = 1