import logging
from common.q3_joiner import Q3Joiner
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    # Crear una instancia de ReleaseDateFilter
    q3_joiner = Q3Joiner(1,1,[])

    # Iniciar el filtro, escuchando mensajes en la cola
    q3_joiner.run()

if __name__ == "__main__":
    main()