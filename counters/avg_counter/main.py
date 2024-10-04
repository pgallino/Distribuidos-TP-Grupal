import logging
from common.avg_counter import AvgCounter
import utils.logging_config # Esto ejecuta la configuración del logger

def main():
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de ReleaseDateFilter
    avg_counter = AvgCounter()

    # Iniciar el filtro, escuchando mensajes en la cola
    avg_counter.run()

if __name__ == "__main__":
    main()