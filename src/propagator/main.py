from utils.utils import NodeType
from utils.initilization import initialize_config, initialize_log
import logging
from propagator import Propagator

def main():

    # Define las claves necesarias para la configuración de este nodo específico
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "trimmer_instances": ("TRIMMER_INSTANCES", "TRIMMER_INSTANCES"),
        "genre_instances": ("GENRE_INSTANCES", "GENRE_INSTANCES"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "release_date_instances": ("RELEASE_DATE_INSTANCES", "RELEASE_DATE_INSTANCES"),
        "english_instances": ("ENGLISH_INSTANCES", "ENGLISH_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
        "propagator_replica_instances": ("PROPAGATOR_REPLICA_INSTANCES", "PROPAGATOR_REPLICA_INSTANCES")
    }

    # Inicializar la configuración
    config_params = initialize_config(required_keys)
    instance_id = config_params["instance_id"]
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Trimmer con los parámetros configurados
    propagator = Propagator(
        instance_id,
        container_name = "propagator",
        nodes_instances = {
            NodeType.TRIMMER.name: config_params["trimmer_instances"],
            NodeType.GENRE.name: config_params["genre_instances"],
            NodeType.SCORE.name: config_params["score_instances"],
            NodeType.RELEASE_DATE.name: config_params["release_date_instances"],
            NodeType.ENGLISH.name: config_params["english_instances"]   
        },
        n_replicas = config_params["propagator_replica_instances"]
    )

    # Iniciar el propagador, escuchando mensajes en la cola
    propagator.run()

if __name__ == "__main__":
    main()
