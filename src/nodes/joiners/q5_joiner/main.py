import logging
from q5_joiner import Q5Joiner
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q5_JOINER_CONFIG_KEYS, Q5_JOINER_CONTAINER_NAME


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(Q5_JOINER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Q5Joiner
    q5_joiner = Q5Joiner(
        id=config_params["instance_id"],
        n_nodes=config_params["q5_joiner_instances"],
        container_name=Q5_JOINER_CONTAINER_NAME,
        n_replicas=config_params["q5_joiner_replica_instances"]
    )

    logging.info(f"Q5Joiner {config_params['instance_id']} iniciado. ")

    # Ejecutar el Q5Joiner
    q5_joiner.run()


if __name__ == "__main__":
    main()
