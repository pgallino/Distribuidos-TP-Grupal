from utils.initilization import initialize_config, initialize_log
from release_date_filter import ReleaseDateFilter

def main():
    # Claves de configuración requeridas para ReleaseDateFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "release_date_instances": ("RELEASE_DATE_INSTANCES", "RELEASE_DATE_INSTANCES"),
        "avg_counter_instances": ("AVG_COUNTER_INSTANCES", "AVG_COUNTER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros del config
    instance_id = config_params["instance_id"]
    release_date_instances = config_params["release_date_instances"]

    # Crear una instancia de ReleaseDateFilter con los parámetros configurados
    release_date_filter = ReleaseDateFilter(
        instance_id,
        release_date_instances,
        [("avg_counter", config_params["avg_counter_instances"])],
        container_name = f"release_date_{instance_id}"
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    release_date_filter.run()

if __name__ == "__main__":
    main()
