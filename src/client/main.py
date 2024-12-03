import logging
from client import Client
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import CLIENT_CONFIG_KEYS


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(CLIENT_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Client
    client = Client(
        id=config_params["instance_id"],
        server_addr=(config_params["server_ip"], config_params["server_port"]),
        max_batch_size=config_params["max_batch_size"],
        games=config_params["games_dataset"],
        reviews=config_params["reviews_dataset"]
    )

    logging.info(f"Cliente {config_params['instance_id']} iniciado.")

    # Ejecutar el cliente
    client.run()


if __name__ == "__main__":
    main()
