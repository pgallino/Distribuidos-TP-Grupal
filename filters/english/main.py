import logging
import os
from common.english_filter import EnglishFilter
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de EnglishFilter
    english_filter = EnglishFilter(int(os.environ['INSTANCE_ID']), int(os.environ['ENGLISH_INSTANCES']))

    # Iniciar el filtro, escuchando mensajes en la cola
    english_filter.run()

if __name__ == "__main__":
    main()
