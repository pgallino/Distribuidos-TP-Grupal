from utils.initilization import initialize_config, initialize_log
from english_filter import EnglishFilter

def main():
    # Claves de configuración requeridas para EnglishFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "english_instances": ("ENGLISH_INSTANCES", "ENGLISH_INSTANCES"),
        "q4_joiner_instances": ("Q4_JOINER_INSTANCES", "Q4_JOINER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    english_instances = config_params["english_instances"]
    q4_joiner_instances = config_params["q4_joiner_instances"]

    # Crear una instancia de EnglishFilter con los parámetros configurados
    english_filter = EnglishFilter(
        instance_id,
        english_instances,
        [('Q4_JOINER', q4_joiner_instances)]  # Configuración de next_nodes con q4_joiner_instances
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    english_filter.run()

if __name__ == "__main__":
    main()
