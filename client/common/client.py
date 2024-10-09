import logging
import signal
import socket
import utils.logging_config # Esto ejecuta la configuración del logger

from messages.messages import Data, Dataset, Fin, Handshake, BATCH_SIZE


class Client:

    def __init__(self, id: int, server_addr: tuple[str, id]):
        self.id = id
        self.server_addr = server_addr
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False

    def send_dataset(self, fname, sock, dataset):
        # chequear si existe el archivo
        with open(fname, mode='r') as file:
            self.logger.custom(f"action: send_handshake | result: success | dataset: {dataset}")
            next(file) # para saltearse el header
            batch = []

            for line in file:
                batch.append(line.strip())  # Agrega la línea al batch
            
                # Cuando el batch alcanza el tamaño deseado, envíalo
                if len(batch) >= BATCH_SIZE:
                    data = Data(self.id, batch, dataset)  # Usa el batch completo
                    sock.send(data.encode())  # Envía el batch codificado
                    # logger.custom(f"action: send_batch | result: success | dataset: {dataset} | batch_size: {BATCH_SIZE}")
                    batch = []  # Limpia el batch para el siguiente grupo
        
        # Enviar el último batch si contiene líneas restantes
            if batch:
                data = Data(self.id, batch, dataset)
                sock.send(data.encode())
                self.logger.custom(f"action: send_last_batch | result: success | dataset: {dataset} | batch_size: {len(batch)}")
            self.logger.custom(f"action: send_data | result: success | dataset: {dataset}")

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._server_socket.close()

    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
    
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(self.server_addr)
            # Envía el mensaje Handshake
            handshake_msg = Handshake(self.id)  # Creamos el mensaje de tipo Handshake con ID 1
            client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_handshake | result: success | message: Handshake")
            
            self.send_dataset("/datasets/reduced_games.csv", client_socket, Dataset(Dataset.GAME))
            self.send_dataset("/datasets/reduced_reviews.csv", client_socket, Dataset(Dataset.REVIEW))
            
            # Envía el mensaje Fin
            fin_msg = Fin(self.id)  # Creamos el mensaje Fin con ID 1
            client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_fin | result: success | message: Fin")

        # except Exception as error:
        #     if not self.shutting_down:
        #         self.logger.custom(f"Error: {error}")
        
        finally:
            client_socket.close()