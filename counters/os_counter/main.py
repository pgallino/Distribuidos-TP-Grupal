import logging
from common.os_counter import OsCounter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de ReleaseDateFilter
    os_counter = OsCounter()

    # Iniciar el filtro, escuchando mensajes en la cola
    os_counter.run()

if __name__ == "__main__":
    main()