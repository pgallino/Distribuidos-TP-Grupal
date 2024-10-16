from utils.initilization import initialize_config
import utils.logging_config  # Esto ejecuta la configuración del logger
from common.os_counter import OsCounter

def main():
    # Claves de configuración requeridas para OsCounter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "os_counter_instances": ("OS_COUNTER_INSTANCES", "OS_COUNTER_INSTANCES")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    os_counter_instances = config_params["os_counter_instances"]

    # Crear una instancia de OsCounter con los parámetros configurados
    os_counter = OsCounter(
        instance_id,
        os_counter_instances,
        []  # Lista vacía para next_nodes
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    os_counter.run()

if __name__ == "__main__":
    main()
