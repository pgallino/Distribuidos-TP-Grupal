from configparser import ConfigParser
import logging
import os
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from common.q4_joiner import Q4Joiner


def initialize_config():
    """ Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file. 
    If at least one of the config parameters is not found a KeyError exception 
    is thrown. If a parameter could not be parsed, a ValueError is thrown. 
    If parsing succeeded, the function returns a ConfigParser object 
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["n_reviews"] = int(os.getenv('N_REVIEWS', config["DEFAULT"]["N_REVIEWS"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():
    
    config_params = initialize_config()
    n_reviews = config_params["n_reviews"]
    # Crear una instancia de Q4Joiner
    joiner = Q4Joiner(n_reviews)

    # Iniciar el filtro, escuchando mensajes en la cola
    joiner.run()

if __name__ == "__main__":
    main()
