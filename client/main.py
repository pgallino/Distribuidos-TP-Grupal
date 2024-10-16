from common.client import Client
from configparser import ConfigParser
import utils.logging_config # Esto ejecuta la configuración del logger
import os

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
        config_params["ip"] = os.getenv('SERVER_IP', config["DEFAULT"]["SERVER_IP"])
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
        config_params["games"] = os.getenv('GAMES_DATASET', config["DEFAULT"]["GAMES_DATASET"])
        config_params["reviews"] = os.getenv('REVIEWS_DATASET', config["DEFAULT"]["REVIEWS_DATASET"])
        config_params["batch"] = int(os.getenv('MAX_BATCH_SIZE', config["DEFAULT"]["MAX_BATCH_SIZE"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def main():

    config_params = initialize_config()
    ip = config_params["ip"]
    port = config_params["port"]
    batch_size = config_params["batch"]
    games = config_params["games"]
    reviews = config_params["reviews"]

    # client = Client(int(os.environ['INSTANCE_ID']), (ip, port), batch_size)
    client = Client(0, (ip, port), batch_size, games, reviews)
    client.run()


if __name__ == "__main__":
    main()