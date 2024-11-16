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
    - required_keys (dict): A dictionary where the key is the parameter name and 
                            the value is a tuple with environment variable name 
                            and an optional default config file key.
                            
    Returns:
    - dict: A dictionary with configuration parameters.
    """
    config = ConfigParser(os.environ)
    config.read("config.ini")
    # if not config.read("config.ini"):
        # print("Advertencia: config.ini no encontrado. Se utilizarán solo variables de entorno.")

    config_params = {}
    for param, (env_var, config_key) in required_keys.items():
        try:
            config_params[param] = os.getenv(env_var) or config.get("DEFAULT", config_key)
            # Convertir a int si el valor es un número
            if isinstance(config_params[param], str) and config_params[param].isdigit():
                config_params[param] = int(config_params[param])
        except KeyError:
            raise KeyError(f"Configuración clave '{config_key}' no encontrada en entorno o archivo. Abortando.")
        except ValueError as e:
            raise ValueError(f"Valor para '{param}' no se pudo interpretar. Error: {e}. Abortando.")
    
    return config_params