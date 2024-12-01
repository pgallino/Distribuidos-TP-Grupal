from q5_joiner import Q5Joiner
from utils.initilization import initialize_config, initialize_log

def main():

    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "q5_joiner_instances": ("Q5_JOINER_INSTANCES", "Q5_JOINER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
        "n_replicas": ("Q5_JOINER_REPLICA_INSTANCES", "Q5_JOINER_REPLICA_INSTANCES")
    }

    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    q5_joiner_instances = config_params["q5_joiner_instances"]
    n_replicas = config_params["n_replicas"]
    # Crear una instancia de ReleaseDateFilter
    q5_joiner = Q5Joiner(
        id = instance_id,
        n_nodes = q5_joiner_instances,
        container_name = "q5_joiner",
        n_replicas = n_replicas
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    q5_joiner.run()

if __name__ == "__main__":
    main()