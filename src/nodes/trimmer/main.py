from utils.initilization import initialize_config, initialize_log
import logging
from trimmer import Trimmer

def main():

    # Define las claves necesarias para la configuración de este nodo específico
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "trimmer_instances": ("TRIMMER_INSTANCES", "TRIMMER_INSTANCES"),
        "genre_instances": ("GENRE_INSTANCES", "GENRE_INSTANCES"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "os_counter_instances": ("OS_COUNTER_INSTANCES", "OS_COUNTER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    # Inicializar la configuración
    config_params = initialize_config(required_keys)
    instance_id = config_params["instance_id"]
    n_nodes = config_params["trimmer_instances"]
    initialize_log(config_params["logging_level"])

    # Crear una instancia de Trimmer con los parámetros configurados
    trimmer = Trimmer(
        instance_id,
        n_nodes,
        [
            ("genre", config_params["genre_instances"]),
            ("score", config_params["score_instances"]),
            ("os_counter", config_params["os_counter_instances"])
        ],
        container_name = f"trimmer_{instance_id}",
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    trimmer.run()

if __name__ == "__main__":
    main()
