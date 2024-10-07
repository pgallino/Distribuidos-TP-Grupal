import socket
import logging
from messages.messages import Handshake, Fin, Data, Dataset
from utils.initilization import initialize_log
import utils.logging_config # Esto ejecuta la configuración del logger

def send_dataset(fname, sock, id, dataset, logger):
    # chequear si existe el archivo
    with open(fname, mode='r') as file:
        logger.custom(f"action: send_handshake | result: success | dataset: {dataset}")
        next(file) # para saltearse el header
        for line in file:
            data = Data(id, line.strip(), dataset)
            sock.send(data.encode())
        logger.custom(f"action: send_data | result: success | dataset: {dataset}")

def main():

    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    logger = logging.getLogger(__name__)
    logger.custom("action: start | result: success")
    
    try:

        client_socket.connect((ip, port))
        # Envía el mensaje Handshake
        handshake_msg = Handshake(1)  # Creamos el mensaje de tipo Handshake con ID 1
        client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
        logger.custom("action: send_handshake | result: success | message: Handshake")
        
        send_dataset("reduced_games.csv", client_socket, 1, Dataset(Dataset.GAME), logger)
        send_dataset("reduced_reviews.csv", client_socket, 1, Dataset(Dataset.REVIEW), logger)
        
        # Envía el mensaje Fin
        fin_msg = Fin(1)  # Creamos el mensaje Fin con ID 1
        client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
        logger.custom("action: send_fin | result: success | message: Fin")

    except Exception as error:
        logger.custom(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()