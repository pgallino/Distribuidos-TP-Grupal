import logging
from common.q5_joiner import Q5Joiner
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    logger = logging.getLogger(__name__)

    logger.info(f"action: start | result: success")

    # Crear una instancia de ReleaseDateFilter
    q5_joiner = Q5Joiner()

    # Iniciar el filtro, escuchando mensajes en la cola
    q5_joiner.run()

if __name__ == "__main__":
    main()