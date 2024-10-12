import logging
import os
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from common.score_filter import ScoreFilter

def main():

    # Crear una instancia de ScoreFilter
    score_filter = ScoreFilter(int(os.environ['INSTANCE_ID']), int(os.environ['SCORE_INSTANCES']), [('JOINER_Q3', 1), ('APPID_COUNTER', 1), ('ENGLISH', int(os.environ['ENGLISH_INSTANCES'])), ('JOINER_Q5', 1)])

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
