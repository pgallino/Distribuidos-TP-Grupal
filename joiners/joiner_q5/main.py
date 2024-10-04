import logging
from common.joiner_q5 import JoinerQ5
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de ReleaseDateFilter
    joiner_q5 = JoinerQ5()

    # Iniciar el filtro, escuchando mensajes en la cola
    joiner_q5.run()

if __name__ == "__main__":
    main()