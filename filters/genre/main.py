import logging
import os
from common.genre_filter import GenreFilter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de GenreFilter
    genre_filter = GenreFilter(
    int(os.environ['INSTANCE_ID']), 
    int(os.environ['GENRE_INSTANCES']), 
    [('RELEASE_DATE', int(os.environ['RELEASE_DATE_INSTANCES'])), ('JOINER_Q3', 1), ('SHOOTER', 1)]
    )


    # Iniciar el filtro, escuchando mensajes en la cola
    genre_filter.run()

if __name__ == "__main__":
    main()