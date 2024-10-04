import logging
from common.q3_joiner import Q3Joiner
import utils.logging_config # Esto ejecuta la configuración del logger

def main():
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de ReleaseDateFilter
    q3_joiner = Q3Joiner()

    # Iniciar el filtro, escuchando mensajes en la cola
    q3_joiner.run()

if __name__ == "__main__":
    main()