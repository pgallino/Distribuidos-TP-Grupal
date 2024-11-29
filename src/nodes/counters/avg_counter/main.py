from utils.initilization import initialize_config, initialize_log
from avg_counter import AvgCounter

def main():
    # Claves de configuración requeridas para AvgCounter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "avg_counter_instances": ("AVG_COUNTER_INSTANCES", "AVG_COUNTER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
        "n_replicas": ("AVG_COUNTER_REPLICA_INSTANCES", "AVG_COUNTER_REPLICA_INSTANCES")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    avg_counter_instances = config_params["avg_counter_instances"]
    n_replicas = config_params["n_replicas"]

    # Crear una instancia de AvgCounter con los parámetros configurados
    avg_counter = AvgCounter(
        id = instance_id,
        n_nodes = avg_counter_instances,
        container_name = "avg_counter",
        n_replicas = n_replicas

    )

    # Iniciar el filtro, escuchando mensajes en la cola
    avg_counter.run()

if __name__ == "__main__":
    main()
