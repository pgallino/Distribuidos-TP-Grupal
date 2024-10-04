import socket
import logging
from messages.messages import Handshake, Fin, Data, GAME_CSV, REVIEW_CSV, OTHER, INDIE, SHOOTER

def main():
    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:

        client_socket.connect((ip, port))
        # Envía el mensaje Handshake
        handshake_msg = Handshake(1)  # Creamos el mensaje de tipo Handshake con ID 1
        client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("Handshake message sent")
        
        # Envía el mensaje Data (game) con el texto "hola esto es un data"
        game_msg = Data(1, "hola esto es un game indie", GAME_CSV, INDIE)  # Creamos el mensaje Data con ID 1, la cadena de texto y el código de game
        client_socket.send(game_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("INDIE sent")

        game_msg = Data(1, "hola esto es un game shooter", GAME_CSV, SHOOTER)  # Creamos el mensaje Data con ID 1, la cadena de texto y el código de game
        client_socket.send(game_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("SHOOTER sent")

        game_msg = Data(1, "hola esto es un game other", GAME_CSV, OTHER)  # Creamos el mensaje Data con ID 1, la cadena de texto y el código de game
        client_socket.send(game_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("OTHER sent")

        # Envía el mensaje Data (review) con el texto "hola esto es un data"
        review_msg = Data(1, "hola esto es una review", REVIEW_CSV, OTHER)  # Creamos el mensaje Data con ID 1, la cadena de texto y el código de review
        client_socket.send(review_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("REVIEW sent")
        
        # Envía el mensaje Fin
        fin_msg = Fin(1)  # Creamos el mensaje Fin con ID 1
        client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
        logging.warning("Fin message sent")

    except Exception as error:
        logging.warning(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()