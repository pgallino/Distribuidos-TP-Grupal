import logging
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from common.q4_joiner import Q4Joiner

def main():

    # Crear una instancia de Q4Joiner
    joiner = Q4Joiner()

    # Iniciar el filtro, escuchando mensajes en la cola
    joiner.run()

if __name__ == "__main__":
    main()
