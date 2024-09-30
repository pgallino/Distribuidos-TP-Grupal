import logging
from common.genre_filter import GenreFilter

def main():
    logging.basicConfig(level=logging.INFO)

    # Crear una instancia de GenreFilter
    genre_filter = GenreFilter()

    # Iniciar el filtro, escuchando mensajes en la cola
    genre_filter.run()

if __name__ == "__main__":
    main()
