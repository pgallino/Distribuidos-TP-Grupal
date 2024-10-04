import logging
from common.genre_filter import GenreFilter
import utils.logging_config # Esto ejecuta la configuración del logger

def main():
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de GenreFilter
    genre_filter = GenreFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    genre_filter.run()

if __name__ == "__main__":
    main()
