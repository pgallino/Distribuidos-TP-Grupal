import logging
import os
from configparser import ConfigParser

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    # Configurar el nivel de logging para pika
    logging.getLogger("pika").setLevel(logging.WARNING)

def initialize_config(required_keys):
    """
    Generalized configuration initializer for nodes with varying environment variables.
    
    Args:
    - required_keys (list): A list of keys representing both environment variables
                            and optional keys in a config file.
                            
    Returns:
    - dict: A dictionary with configuration parameters.
    """
    config = ConfigParser(os.environ)
    config.read("config.ini")
    # if not config.read("config.ini"):
        # print("Advertencia: config.ini no encontrado. Se utilizarán solo variables de entorno.")

    config_params = {}
    for key in required_keys:
        try:
            # Intentar obtener el valor desde el entorno o desde el archivo de configuración
            config_params[key] = os.getenv(key.upper()) or config.get("DEFAULT", key.upper())
            # Convertir a int si el valor es un número
            if isinstance(config_params[key], str) and config_params[key].isdigit():
                config_params[key] = int(config_params[key])
        except KeyError:
            raise KeyError(f"Configuración clave '{key}' no encontrada en entorno o archivo. Abortando.")
        except ValueError as e:
            raise ValueError(f"Valor para '{key}' no se pudo interpretar. Error: {e}. Abortando.")
    
    return config_params
