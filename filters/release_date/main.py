import logging
import os
from common.release_date_filter import ReleaseDateFilter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de ReleaseDateFilter
    release_date_filter = ReleaseDateFilter(int(os.environ['INSTANCE_ID']), int(os.environ['RELEASE_DATE_INSTANCES']))

    # Iniciar el filtro, escuchando mensajes en la cola
    release_date_filter.run()

if __name__ == "__main__":
    main()
