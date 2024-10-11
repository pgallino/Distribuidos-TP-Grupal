import logging
import os
import utils.logging_config # Esto ejecuta la configuración del logger
from common.trimmer import Trimmer

def main():

    # Crear una instancia de Trimmer
    trimmer = Trimmer(int(os.environ['INSTANCE_ID']), int(os.environ['TRIMMER_INSTANCES']), [('GENRE', int(os.environ['GENRE_INSTANCES'])), ('SCORE', int(os.environ['SCORE_INSTANCES'])), ('OS_COUNTER', int(os.environ['OS_COUNTER_INSTANCES']))])

    # Iniciar el filtro, escuchando mensajes en la cola
    trimmer.run()

if __name__ == "__main__":
    main()
