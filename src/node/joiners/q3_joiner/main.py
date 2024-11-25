from utils.initilization import initialize_config, initialize_log
from q3_joiner import Q3Joiner

def main():

    # Claves de configuración requeridas para Q3Joiner
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "q3_joiner_instances": ("Q3_JOINER_INSTANCES", "Q3_JOINER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
        "n_replicas": ("Q3_JOINER_REPLICA_INSTANCES", "Q3_JOINER_REPLICA_INSTANCES")
    }
    
    # Inicializar configuración
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros de config
    instance_id = config_params["instance_id"]
    q3_joiner_instances = config_params["q3_joiner_instances"]
    n_replicas = config_params["n_replicas"]

    # Crear una instancia de Q3Joiner con los parámetros configurados
    q3_joiner = Q3Joiner(
        id = instance_id,
        n_nodes = q3_joiner_instances,
        container_name = f"q3_joiner_{instance_id}",
        n_replicas = n_replicas
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    q3_joiner.run()

if __name__ == "__main__":
    main()
