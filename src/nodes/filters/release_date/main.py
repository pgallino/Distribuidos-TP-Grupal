import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import RELEASE_DATE_FILTER_CONFIG_KEYS, RELEASE_DATE_FILTER_NEXT_NODES, RELEASE_DATE_CONTAINER_NAME
from release_date_filter import ReleaseDateFilter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(RELEASE_DATE_FILTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para los nodos siguientes
    next_nodes = [
        (node, config_params[f"{node}_instances"])
        for node in RELEASE_DATE_FILTER_NEXT_NODES
    ]

    # Crear una instancia de ReleaseDateFilter
    release_date_filter = ReleaseDateFilter(
        id=config_params["instance_id"],
        n_nodes=config_params["release_date_instances"],
        n_next_nodes=next_nodes,
        container_name=RELEASE_DATE_CONTAINER_NAME
    )

    logging.info(f"ReleaseDateFilter {config_params['instance_id']} iniciado. ")

    # Ejecutar el ReleaseDateFilter
    release_date_filter.run()


if __name__ == "__main__":
    main()
