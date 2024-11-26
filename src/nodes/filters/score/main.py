from utils.initilization import initialize_config, initialize_log
from score_filter import ScoreFilter

def main():
    # Claves de configuración requeridas para ScoreFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "q3_joiner_instances": ("Q3_JOINER_INSTANCES", "Q3_JOINER_INSTANCES"),
        "q4_joiner_instances": ("Q4_JOINER_INSTANCES", "Q4_JOINER_INSTANCES"),
        "q5_joiner_instances": ("Q5_JOINER_INSTANCES", "Q5_JOINER_INSTANCES"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }
    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log(config_params["logging_level"])

    # Extraer parámetros del config
    instance_id = config_params["instance_id"]
    score_instances = config_params["score_instances"]
    joiner_q3_instances = config_params["q3_joiner_instances"]
    joiner_q4_instances = config_params["q4_joiner_instances"]
    joiner_q5_instances = config_params["q5_joiner_instances"]

    # Crear una instancia de ScoreFilter con los parámetros configurados
    score_filter = ScoreFilter(
        instance_id,
        score_instances,
        [('q3_joiner', joiner_q3_instances), ('q4_joiner', joiner_q4_instances), ('q5_joiner', joiner_q5_instances)],
        container_name = f"score_{instance_id}"
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
