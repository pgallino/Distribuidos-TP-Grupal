import socket
import logging
from messages.messages import encode_handshake, encode_data, encode_fin

def main():
    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:

        client_socket.connect((ip, port))
        # Envía el mensaje Handshake
        print("arranco aca")
        handshake = encode_handshake(1)
        print("encodeeeeeeee")
        client_socket.send(encode_handshake(1))

        # Envía el mensaje Data con el texto "hola esto es un data"
        client_socket.send(encode_data(1, "hola esto es un data"))
        print("Data message sent")

        # Envía el mensaje Fin
        client_socket.send(encode_fin(1))
        print("Fin message sent")

    except Exception as error:
        logging.info(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()