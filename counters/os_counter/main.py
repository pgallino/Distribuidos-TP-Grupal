import logging
from common.os_counter import OsCounter
import utils.logging_config # Esto ejecuta la configuración del logger

def main():
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de ReleaseDateFilter
    os_counter = OsCounter()

    # Iniciar el filtro, escuchando mensajes en la cola
    os_counter.run()

if __name__ == "__main__":
    main()