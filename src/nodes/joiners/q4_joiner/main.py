import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q4_JOINER_CONFIG_KEYS, Q4_JOINER_CONTAINER_NAME, ENGLISH_CONTAINER_NAME
from q4_joiner import Q4Joiner


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(Q4_JOINER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para los nodos siguientes
    next_nodes = [
        (ENGLISH_CONTAINER_NAME, config_params["english_instances"])
    ]

    # Crear una instancia de Q4Joiner
    joiner = Q4Joiner(
        id=config_params["instance_id"],
        n_nodes=config_params["q4_joiner_instances"],
        n_next_nodes=next_nodes,
        batch_size=config_params["max_batch_size"],
        n_reviews=config_params["n_reviews"],
        container_name=Q4_JOINER_CONTAINER_NAME,
        n_replicas=config_params["q4_joiner_replica_instances"]
    )

    logging.info(f"Q4Joiner {config_params['instance_id']} iniciado. ")

    # Ejecutar el Q4Joiner
    joiner.run()


if __name__ == "__main__":
    main()
