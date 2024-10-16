from utils.initilization import initialize_config, initialize_log
import utils.logging_config  # Esto ejecuta la configuración del logger
from common.score_filter import ScoreFilter

def main():
    # Claves de configuración requeridas para ScoreFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "score_instances": ("SCORE_INSTANCES", "SCORE_INSTANCES"),
        "joiner_q3_instances": ("JOINER_Q3_INSTANCES", "JOINER_Q3_INSTANCES"),
        "joiner_q4_instances": ("JOINER_Q4_INSTANCES", "JOINER_Q4_INSTANCES"),
        "joiner_q5_instances": ("JOINER_Q5_INSTANCES", "JOINER_Q5_INSTANCES"),
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)
    initialize_log()

    # Extraer parámetros del config
    instance_id = config_params["instance_id"]
    score_instances = config_params["score_instances"]
    joiner_q3_instances = config_params["joiner_q3_instances"]
    joiner_q4_instances = config_params["joiner_q4_instances"]
    joiner_q5_instances = config_params["joiner_q5_instances"]

    # Crear una instancia de ScoreFilter con los parámetros configurados
    score_filter = ScoreFilter(
        instance_id,
        score_instances,
        [('JOINER_Q3', joiner_q3_instances), ('JOINER_Q4', joiner_q4_instances), ('JOINER_Q5', joiner_q5_instances)]
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
