import logging
import utils.logging_config # Esto ejecuta la configuración del logger
from common.trimmer import Trimmer

def main():

    # Crear una instancia de Trimmer
    trimmer = Trimmer()

    # Iniciar el filtro, escuchando mensajes en la cola
    trimmer.run()

if __name__ == "__main__":
    main()
