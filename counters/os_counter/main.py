import logging
from common.os_counter import OsCounter

def main():
    logging.basicConfig(level="WARNING")

    # Crear una instancia de ReleaseDateFilter
    os_counter = OsCounter()

    # Iniciar el filtro, escuchando mensajes en la cola
    os_counter.run()

if __name__ == "__main__":
    main()