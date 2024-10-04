#!/usr/bin/env python3
from common.server import Server
import logging
from utils.initilization import initialize_log, initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger


def main():
    config_params = initialize_config()
    port = config_params["port"]
    listen_backlog = config_params["listen_backlog"]


    # Log config parameters at the beginning of the program to verify the configuration
    # of the component
    # logging.info(f"action: config | result: success | port: {port} | "
    #               f"listen_backlog: {listen_backlog} | logging_level: {logging_level}")
    
    # Obtener el logger configurado
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")
    
    # Initialize server and start server loop
    server = Server(port, listen_backlog)
    server.run()


if __name__ == "__main__":
    main()
