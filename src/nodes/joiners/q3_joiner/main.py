import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q3_JOINER_CONFIG_KEYS, Q3_JOINER_CONTAINER_NAME
from q3_joiner import Q3Joiner


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(Q3_JOINER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Q3Joiner
    q3_joiner = Q3Joiner(
        id=config_params["instance_id"],
        n_nodes=config_params["q3_joiner_instances"],
        container_name=Q3_JOINER_CONTAINER_NAME,
        n_replicas=config_params["q3_joiner_replica_instances"]
    )

    logging.info(f"Q3Joiner {config_params['instance_id']} iniciado. ")

    # Ejecutar el Q3Joiner
    q3_joiner.run()


if __name__ == "__main__":
    main()
