from utils.initilization import initialize_config
import utils.logging_config  # Esto ejecuta la configuración del logger
from q3_joiner import Q3Joiner

def main():

    # Claves de configuración requeridas para Q3Joiner
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "q3_joiner_instances": ("Q3_JOINER_INSTANCES", "Q3_JOINER_INSTANCES"),
    }
    
    # Inicializar configuración
    config_params = initialize_config(required_keys)

    # Extraer parámetros de config
    instance_id = config_params["instance_id"]
    q3_joiner_instances = config_params["q3_joiner_instances"]

    # Crear una instancia de Q3Joiner con los parámetros configurados
    q3_joiner = Q3Joiner(
        instance_id,
        q3_joiner_instances,
        []
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    q3_joiner.run()

if __name__ == "__main__":
    main()
