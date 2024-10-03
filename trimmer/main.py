import logging
from common.trimmer import Trimmer

def main():
    logging.basicConfig(level="WARNING")

    # Crear una instancia de Trimmer
    score_filter = Trimmer()

    # Iniciar el filtro, escuchando mensajes en la cola
    score_filter.run()

if __name__ == "__main__":
    main()
