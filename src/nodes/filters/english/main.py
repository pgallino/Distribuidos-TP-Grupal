import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import ENGLISH_FILTER_CONFIG_KEYS, ENGLISH_FILTER_NEXT_NODES, ENGLISH_CONTAINER_NAME
from english_filter import EnglishFilter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(ENGLISH_FILTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para los nodos siguientes
    next_nodes = [
        (node, config_params[f"{node}_instances"])
        for node in ENGLISH_FILTER_NEXT_NODES
    ]

    # Crear una instancia de EnglishFilter
    english_filter = EnglishFilter(
        id=config_params["instance_id"],
        n_nodes=config_params["english_instances"],
        n_next_nodes=next_nodes,
        container_name=ENGLISH_CONTAINER_NAME
    )

    logging.info(f"EnglishFilter {config_params['instance_id']} iniciado.")

    # Ejecutar el EnglishFilter
    english_filter.run()


if __name__ == "__main__":
    main()
