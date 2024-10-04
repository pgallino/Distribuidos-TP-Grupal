import logging
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from joiners.joiner_q4.common.joiner_q4 import JoinerQ4

def main():

    # Obtener el logger configurado
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de JoinerQ4
    joiner = JoinerQ4()

    # Iniciar el filtro, escuchando mensajes en la cola
    joiner.run()

if __name__ == "__main__":
    main()
