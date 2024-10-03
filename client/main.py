import socket
import logging
from messages.messages import Handshake, Fin, Data

def main():
    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:

        client_socket.connect((ip, port))
        # Envía el mensaje Handshake
        handshake_msg = Handshake(1)  # Creamos el mensaje de tipo Handshake con ID 1
        client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
        print("Handshake message sent")
        
        # Envía el mensaje Data con el texto "hola esto es un data"
        data_msg = Data(1, "hola esto es un data")  # Creamos el mensaje Data con ID 1 y la cadena de texto
        client_socket.send(data_msg.encode())  # Codificamos y enviamos el mensaje
        print("Data message sent")
        
        # Envía el mensaje Fin
        fin_msg = Fin(1)  # Creamos el mensaje Fin con ID 1
        client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
        print("Fin message sent")

    except Exception as error:
        logging.info(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()