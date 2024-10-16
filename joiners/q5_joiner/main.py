import logging
from common.q5_joiner import Q5Joiner
from utils.initilization import initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "q5_joiner_instances": ("Q5_JOINER_INSTANCES", "Q5_JOINER_INSTANCES"),
    }

    config_params = initialize_config(required_keys)

    # Crear una instancia de ReleaseDateFilter
    q5_joiner = Q5Joiner(
        config_params["instance_id"],
        config_params["q5_joiner_instances"],
        []
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    q5_joiner.run()

if __name__ == "__main__":
    main()