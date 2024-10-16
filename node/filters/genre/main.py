from genre_filter import GenreFilter
from utils.initilization import initialize_config
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Claves de configuración requeridas para GenreFilter
    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "genre_instances": ("GENRE_INSTANCES", "GENRE_INSTANCES"),
        "release_date_instances": ("RELEASE_DATE_INSTANCES", "RELEASE_DATE_INSTANCES"),
        "q3_joiner_instances": ("Q3_JOINER_INSTANCES", "Q3_JOINER_INSTANCES"),
        "q4_joiner_instances": ("Q4_JOINER_INSTANCES", "Q4_JOINER_INSTANCES"),
        "q5_joiner_instances": ("Q5_JOINER_INSTANCES", "Q5_JOINER_INSTANCES")
    }

    # Inicializar configuración y logging
    config_params = initialize_config(required_keys)

    # Extraer parámetros de configuración
    instance_id = config_params["instance_id"]
    genre_instances = config_params["genre_instances"]
    release_date_instances = config_params["release_date_instances"]
    q3_joiner_instances = config_params["q3_joiner_instances"]
    q4_joiner_instances = config_params["q4_joiner_instances"]
    q5_joiner_instances = config_params["q5_joiner_instances"]

    # Crear una instancia de GenreFilter con los parámetros configurados
    genre_filter = GenreFilter(
        instance_id,
        genre_instances,
        [
            ('RELEASE_DATE', release_date_instances),
            ('JOINER_Q3', q3_joiner_instances),
            ('JOINER_Q4', q4_joiner_instances),
            ('JOINER_Q5', q5_joiner_instances)
        ]
    )

    # Iniciar el filtro, escuchando mensajes en la cola
    genre_filter.run()

if __name__ == "__main__":
    main()