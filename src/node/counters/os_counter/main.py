from utils.initilization import initialize_config, initialize_log
from os_counter import OsCounter

def main():
    # Claves de configuración requeridas para OsCounter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "os_counter_instances": ("OS_COUNTER_INSTANCES", "OS_COUNTER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    os_counter_instances = config_params["os_counter_instances"]

    # Crear una instancia de OsCounter con los parámetros configurados
    os_counter = OsCounter(
        id = instance_id,
        n_nodes = os_counter_instances,
        container_name = f"os_counter_{instance_id}"
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    os_counter.run()

if __name__ == "__main__":
    main()
