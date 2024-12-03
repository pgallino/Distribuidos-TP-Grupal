import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import AVG_COUNTER_CONFIG_KEYS, AVG_COUNTER_CONTAINER_NAME
from avg_counter import AvgCounter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(AVG_COUNTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear una instancia de AvgCounter
    avg_counter = AvgCounter(
        id=config_params["instance_id"],
        n_nodes=config_params["avg_counter_instances"],
        container_name=AVG_COUNTER_CONTAINER_NAME,
        n_replicas=config_params["avg_counter_replica_instances"]
    )

    logging.info(f"AvgCounter {config_params['instance_id']} iniciado. Esperando mensajes...")

    # Ejecutar el AvgCounter
    avg_counter.run()


if __name__ == "__main__":
    main()
