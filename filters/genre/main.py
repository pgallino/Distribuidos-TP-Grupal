import logging
from common.genre_filter import GenreFilter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de GenreFilter
    genre_filter = GenreFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    genre_filter.run()

if __name__ == "__main__":
    main()
