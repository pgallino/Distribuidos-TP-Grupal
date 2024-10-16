from utils.initilization import initialize_config
import utils.logging_config  # Esto ejecuta la configuración del logger
from avg_counter import AvgCounter

def main():
    # Claves de configuración requeridas para AvgCounter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "avg_counter_instances": ("AVG_COUNTER_INSTANCES", "AVG_COUNTER_INSTANCES")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    avg_counter_instances = config_params["avg_counter_instances"]

    # Crear una instancia de AvgCounter con los parámetros configurados
    avg_counter = AvgCounter(
        instance_id,
        avg_counter_instances,
        []  # Lista vacía para next_nodes
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    avg_counter.run()

if __name__ == "__main__":
    main()
