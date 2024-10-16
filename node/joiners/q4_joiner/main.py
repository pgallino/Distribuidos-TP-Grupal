from utils.initilization import initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger
from q4_joiner import Q4Joiner

def main():

    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "q4_joiner_instances": ("Q4_JOINER_INSTANCES", "Q4_JOINER_INSTANCES"),
        "n_reviews": ("N_REVIEWS", "N_REVIEWS"),
        "batch": ("MAX_BATCH_SIZE", "MAX_BATCH_SIZE"),
        "english_instances": ("ENGLISH_INSTANCES", "ENGLISH_INSTANCES"),
    }
    
    config_params = initialize_config(required_keys)

    # Extraer parámetros del config
    instance_id = config_params["instance_id"]
    q4_joiner_instances = config_params["q4_joiner_instances"]
    n_reviews = config_params["n_reviews"]
    batch = config_params["batch"]
    english_instances = config_params["english_instances"]

    joiner = Q4Joiner(
        instance_id,
        q4_joiner_instances,
        [('ENGLISH', english_instances)],
        batch,
        n_reviews
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    joiner.run()

if __name__ == "__main__":
    main()
