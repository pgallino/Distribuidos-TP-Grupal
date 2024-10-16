from utils.initilization import initialize_config, initialize_log
import utils.logging_config  # Esto ejecuta la configuración del logger
from common.release_date_filter import ReleaseDateFilter

def main():
    # Claves de configuración requeridas para ReleaseDateFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "release_date_instances": ("RELEASE_DATE_INSTANCES", "RELEASE_DATE_INSTANCES"),
        "avg_counter_instances": ("AVG_COUNTER_INSTANCES", "AVG_COUNTER_INSTANCES")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log()

    # Extraer parámetros del config
    instance_id = config_params["instance_id"]
    release_date_instances = config_params["release_date_instances"]

    # Crear una instancia de ReleaseDateFilter con los parámetros configurados
    release_date_filter = ReleaseDateFilter(
        instance_id,
        release_date_instances,
        [("AVG_COUNTER", config_params["avg_counter_instances"])]
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    release_date_filter.run()

if __name__ == "__main__":
    main()
