import logging
import os
from common.appids_counter import AppIdsCounter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de ReleaseDateFilter
    apps_ids_counter = AppIdsCounter(1, 1, [('ENGLISH', int(os.environ['ENGLISH_INSTANCES']))])

    # Iniciar el filtro, escuchando mensajes en la cola
    apps_ids_counter.run()

if __name__ == "__main__":
    main()