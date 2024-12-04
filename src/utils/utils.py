from enum import Enum
import logging
import struct
import subprocess
import time
import inspect

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

def reanimate_container(container_name, sleep_seconds=1):
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
            return False
        else:
            logging.info(f"Container {container_name} reanimated successfully.")
            return True

    except Exception as e:
        logging.error(f"Unexpected error while handling container {container_name}: {e}")

class TaskType(Enum):
    PULL = 0
    REANIMATE_MASTER = 1

class NodeType(Enum):
    TRIMMER = 0
    GENRE = 1
    SCORE = 2
    ENGLISH = 3
    RELEASE_DATE = 4
    OS_COUNTER = 5
    AVG_COUNTER = 6
    Q3_JOINER = 7
    Q4_JOINER = 8
    Q5_JOINER = 9
    OS_COUNTER_REPLICA = 10
    AVG_COUNTER_REPLICA = 11
    Q3_JOINER_REPLICA = 12
    Q4_JOINER_REPLICA = 13
    Q5_JOINER_REPLICA = 14

    def string_to_node_type(node_type_str: str) -> 'NodeType':
        """
        Convierte una cadena a un miembro del enumerador NodeType.

        :param node_type_str: La cadena que representa el nombre del NodeType.
        :return: El miembro correspondiente de NodeType.
        :raises ValueError: Si la cadena no corresponde a ningún miembro de NodeType.
        """
        node_type_str = node_type_str.lower()
        if node_type_str in _STRING_TO_NODE_TYPE:
            return _STRING_TO_NODE_TYPE[node_type_str]
        raise ValueError(f"'{node_type_str}' no es un tipo de nodo válido en NodeType.")
        
    @staticmethod
    def node_type_to_string(node_type: 'NodeType') -> str:
        """
        Convierte un miembro de NodeType a su representación en cadena.

        :param node_type: Miembro de NodeType.
        :return: La representación en cadena del miembro de NodeType.
        :raises ValueError: Si el argumento no es un miembro de NodeType.
        """
        if not isinstance(node_type, NodeType):
            raise ValueError(f"'{node_type}' no es un miembro válido de NodeType.")
        return node_type.name.lower()

    def get_next_nodes(node_type: 'NodeType') -> list['NodeType']:
        if node_type == NodeType.TRIMMER:
            return [NodeType.GENRE, NodeType.SCORE, NodeType.OS_COUNTER]
        if node_type == NodeType.GENRE:
            return [NodeType.RELEASE_DATE, NodeType.Q3_JOINER, NodeType.Q4_JOINER, NodeType.Q5_JOINER]
        if node_type == NodeType.SCORE:
            return [NodeType.Q3_JOINER, NodeType.Q4_JOINER, NodeType.Q5_JOINER]
        if node_type == NodeType.RELEASE_DATE:
            return [NodeType.AVG_COUNTER]
        if node_type == NodeType.ENGLISH:
            return [NodeType.Q4_JOINER]
        raise ValueError(f"'{NodeType}' no tiene un nodo siguiente en el pipeline.")

# Diccionario para mapeo manual
_STRING_TO_NODE_TYPE = {
    "trimmer": NodeType.TRIMMER,
    "genre": NodeType.GENRE,
    "score": NodeType.SCORE,
    "english": NodeType.ENGLISH,
    "release_date": NodeType.RELEASE_DATE,
    "os_counter": NodeType.OS_COUNTER,
    "avg_counter": NodeType.AVG_COUNTER,
    "q3_joiner": NodeType.Q3_JOINER,
    "q4_joiner": NodeType.Q4_JOINER,
    "q5_joiner": NodeType.Q5_JOINER,
    "os_counter_replica": NodeType.OS_COUNTER_REPLICA,
    "avg_counter_replica": NodeType.AVG_COUNTER_REPLICA,
    "q3_joiner_replica": NodeType.Q3_JOINER_REPLICA,
    "q4_joiner_replica": NodeType.Q4_JOINER_REPLICA,
    "q5_joiner_replica": NodeType.Q5_JOINER_REPLICA,
}

def log_with_location(message):
    """Log message with dynamic file name and line number."""
    frame = inspect.currentframe().f_back
    file_name = frame.f_code.co_filename
    line_number = frame.f_lineno
    return f"{message} - {file_name} - LINE {line_number}"

import random

def simulate_random_failure(node, log_message, probability=0.1):
    """
    Simula una caída del sistema con una probabilidad dada.
    
    Args:
        node (node): La instancia de la réplica/nodo en la que se ejecuta la simulación.
        log_message (str): Mensaje de log que describe la simulación.
        probability (float): Probabilidad de simular una caída (entre 0 y 1).
    """
    replicas = {NodeType.AVG_COUNTER_REPLICA, NodeType.OS_COUNTER_REPLICA, NodeType.Q3_JOINER_REPLICA, NodeType.Q4_JOINER_REPLICA, NodeType.Q5_JOINER_REPLICA}
    masters = {NodeType.AVG_COUNTER, NodeType.OS_COUNTER, NodeType.Q3_JOINER, NodeType.Q4_JOINER, NodeType.Q5_JOINER}
    if (node.get_type() in replicas) and node.id == 1: #SI ES EL NODO 1 NO LO TIRO NUNCA SI ES UNA REPLICA
        return

    # Verificar si el nodo inició hace menos de 5 segundos
    current_time = time.time()
    time_since_start = current_time - node.timestamp  # Calcular tiempo transcurrido

    if time_since_start < 60:  # Si han pasado menos de 5 segundos, no simular fallo
        return
    
    # Si es un master y no tiene réplicas, no simular fallo
    if (node.get_type() in masters) and node.n_replicas == 0:
        return

    if random.random() < probability:
        logging.warning(f"Simulando caída con probabilidad {probability * 100}% en la réplica {node.id}.")
        logging.warning(log_message)
        node._shutdown()
        exit(0)
