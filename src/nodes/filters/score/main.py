import logging
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import SCORE_FILTER_CONFIG_KEYS, SCORE_FILTER_NEXT_NODES, SCORE_CONTAINER_NAME
from score_filter import ScoreFilter


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(SCORE_FILTER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para los nodos siguientes
    next_nodes = [
        (node, config_params[f"{node}_instances"])
        for node in SCORE_FILTER_NEXT_NODES
    ]

    # Crear una instancia de ScoreFilter
    score_filter = ScoreFilter(
        id=config_params["instance_id"],
        n_nodes=config_params["score_instances"],
        n_next_nodes=next_nodes,
        container_name=SCORE_CONTAINER_NAME
    )

    logging.info(f"ScoreFilter {config_params['instance_id']} iniciado. ")

    # Ejecutar el ScoreFilter
    score_filter.run()


if __name__ == "__main__":
    main()
