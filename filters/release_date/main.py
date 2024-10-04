import logging
from common.release_date_filter import ReleaseDateFilter

def main():
    logging.basicConfig(level="WARNING")

    # Crear una instancia de ReleaseDateFilter
    release_date_filter = ReleaseDateFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    release_date_filter.run()

if __name__ == "__main__":
    main()
