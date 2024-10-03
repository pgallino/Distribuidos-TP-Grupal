import logging
from common.score_filter import ScoreFilter

def main():
    logging.basicConfig(level="WARNING")

    # Crear una instancia de ScoreFilter
    score_filter = ScoreFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
