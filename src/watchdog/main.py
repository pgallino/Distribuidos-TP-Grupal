from messages.messages import NodeType
from utils.initilization import initialize_config, initialize_log
import logging
from watchdog import WatchDog

def main():

    # Define las claves necesarias para la configuración de este nodo específico
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "watchdog_instances": ("WATCHDOG_INSTANCES", "WATCHDOG_INSTANCES"),
        "trimmer_instances": ("TRIMMER_INSTANCES", "TRIMMER_INSTANCES"),
        "genre_instances": ("GENRE_INSTANCES", "GENRE_INSTANCES"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "release_date_instances": ("RELEASE_DATE_INSTANCES", "RELEASE_DATE_INSTANCES"),
        "english_instances": ("ENGLISH_INSTANCES", "ENGLISH_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    # Inicializar la configuración
    config_params = initialize_config(required_keys)
    instance_id = config_params["instance_id"]
    n_nodes = config_params["watchdog_instances"]
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Trimmer con los parámetros configurados
    watchdog = WatchDog(
        instance_id,
        n_nodes,
        container_name = f"watchdog_{instance_id}",
        n_nodes_instances = [
            (NodeType.string_to_node_type("trimmer"), config_params["trimmer_instances"]),
            (NodeType.string_to_node_type("genre"), config_params["genre_instances"]),
            (NodeType.string_to_node_type("score"), config_params["score_instances"]),
            (NodeType.string_to_node_type("release_date"), config_params["release_date_instances"]),
            (NodeType.string_to_node_type("english"), config_params["english_instances"]),
            
        ]
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    watchdog.run()

if __name__ == "__main__":
    main()
