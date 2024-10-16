from utils.initilization import initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger
from trimmer import Trimmer

def main():

    # Define las claves necesarias para la configuración de este nodo específico
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "trimmer_instances": ("TRIMMER_INSTANCES", "TRIMMER_INSTANCES"),
        "genre_instances": ("GENRE_INSTANCES", "GENRE_INSTANCES"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "os_counter_instances": ("OS_COUNTER_INSTANCES", "OS_COUNTER_INSTANCES")
    }

    # Inicializar la configuración
    config_params = initialize_config(required_keys)

    # Crear una instancia de Trimmer con los parámetros configurados
    trimmer = Trimmer(
        config_params["instance_id"],
        config_params["trimmer_instances"],
        [
            ("GENRE", config_params["genre_instances"]),
            ("SCORE", config_params["score_instances"]),
            ("OS_COUNTER", config_params["os_counter_instances"])
        ]
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    trimmer.run()

if __name__ == "__main__":
    main()
