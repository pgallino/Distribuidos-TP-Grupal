from utils.container_constants import SERVER_CONFIG_KEYS
from utils.initilization import initialize_config, initialize_log
from server import Server
import logging


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(SERVER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])
    
    logging.info(f"action: start | result: success")
    
    # Inicializar servidor y ejecutar el bucle principal
    server = Server(
        port=config_params["server_port"],
        listen_backlog=config_params["server_listen_backlog"],
        n_next_nodes=config_params["trimmer_instances"]
    )
    server.run()


if __name__ == "__main__":
    main()
