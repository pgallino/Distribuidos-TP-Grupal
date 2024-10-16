#!/usr/bin/env python3
from server import Server
import logging
from utils.initilization import initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger


def main():
    required_keys = {
        "port": ("SERVER_PORT", "SERVER_PORT"),
        "listen_backlog": ("SERVER_LISTEN_BACKLOG", "SERVER_LISTEN_BACKLOG"),
        "trimmer_instances": ("TRIMMER_INSTANCES", "TRIMMER_INSTANCES")
    }
    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    
    # Obtener el logger configurado
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")
    
    # Initialize server and start server loop
    server = Server(config_params["port"], config_params["listen_backlog"], config_params["trimmer_instances"])
    server.run()


if __name__ == "__main__":
    main()
