import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import OS_COUNTER_CONFIG_KEYS, OS_COUNTER_CONTAINER_NAME
from os_counter import OsCounter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(OS_COUNTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear una instancia de OsCounter
    os_counter = OsCounter(
        id=config_params["instance_id"],
        n_nodes=config_params["os_counter_instances"],
        container_name=OS_COUNTER_CONTAINER_NAME,
        n_replicas=config_params["os_counter_replica_instances"]
    )

    logging.info(f"OsCounter {config_params['instance_id']} iniciado.")

    # Ejecutar el OsCounter
    os_counter.run()


if __name__ == "__main__":
    main()
