import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import GENRE_FILTER_CONFIG_KEYS, GENRE_FILTER_NEXT_NODES, GENRE_CONTAINER_NAME
from genre_filter import GenreFilter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(GENRE_FILTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para los nodos siguientes
    next_nodes = [
        (node, config_params[f"{node}_instances"])
        for node in GENRE_FILTER_NEXT_NODES
    ]

    # Crear una instancia de GenreFilter
    genre_filter = GenreFilter(
        id=config_params["instance_id"],
        n_nodes=config_params["genre_instances"],
        n_next_nodes=next_nodes,
        container_name=GENRE_CONTAINER_NAME
    )

    logging.info(f"GenreFilter {config_params['instance_id']} iniciado. ")

    # Ejecutar el GenreFilter
    genre_filter.run()


if __name__ == "__main__":
    main()
