import logging
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger
from common.score_filter import ScoreFilter

def main():

    # Crear una instancia de ScoreFilter
    score_filter = ScoreFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
