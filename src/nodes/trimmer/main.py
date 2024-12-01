from utils.initilization import initialize_config, initialize_log
from utils.container_constants import TRIMMER_CONFIG_KEYS, TRIMMER_CONTAINER_NAME, TRIMMER_NEXT_NODES
from trimmer import Trimmer
import logging


def main():
    # Inicializar configuración y logging
    config_params = initialize_config(TRIMMER_CONFIG_KEYS)
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para next_nodes
    next_nodes = [
        (node, config_params[f"{node}_instances"])
        for node in TRIMMER_NEXT_NODES
    ]

    # Crear una instancia de Trimmer
    trimmer = Trimmer(
        id=config_params["instance_id"],
        n_nodes=config_params["trimmer_instances"],
        n_next_nodes=next_nodes,
        container_name=TRIMMER_CONTAINER_NAME
    )

    logging.info(f"Trimmer {config_params['instance_id']} iniciado. ")

    # Ejecutar el Trimmer
    trimmer.run()


if __name__ == "__main__":
    main()

