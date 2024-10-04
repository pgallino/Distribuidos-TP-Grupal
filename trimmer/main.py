import logging
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from common.trimmer import Trimmer

def main():

    # Obtener el logger configurado
    logger = logging.getLogger(__name__)
    logger.info(f"action: start | result: success")

    # Crear una instancia de Trimmer
    trimmer = Trimmer()

    # Iniciar el filtro, escuchando mensajes en la cola
    trimmer.run()

if __name__ == "__main__":
    main()
