import logging
import signal
import socket
import utils.logging_config # Esto ejecuta la configuración del logger

from messages.messages import Data, Dataset, Fin, Handshake, BATCH_SIZE, MsgType, QueryNumber, decode_msg
from utils.utils import recv_msg

BATCH_SIZE_BYTES = 8192  # 8 KB



class Client:

    def __init__(self, id: int, server_addr: tuple[str, id]):
        self.id = id
        self.server_addr = server_addr
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_dataset(self, fname, sock, dataset):
        # chequear si existe el archivo
        with open(fname, mode='r') as file:
            self.logger.custom(f"action: send_handshake | result: success | dataset: {dataset}")
            next(file) # para saltearse el header
            batch = []
            current_batch_size = 0  # Tamaño actual del batch en bytes

            for line in file:
                line = line.strip()
                line_size = len(line.encode('utf-8'))  # Tamaño de la línea en bytes
                batch.append(line)
                current_batch_size += line_size  # Suma el tamaño de la línea al batch
                
                # Cuando el batch alcanza o supera los 8 KB, envíalo
                if current_batch_size >= BATCH_SIZE_BYTES:
                    data = Data(self.id, batch, dataset)  # Usa el batch completo
                    sock.sendall(data.encode())  # Envía el batch codificado
                    # self.logger.custom(f"action: send_batch | result: success | dataset: {dataset} | batch_size: {current_batch_size} bytes | lines: {len(batch)}")
                    
                    # Reinicia el batch y el contador de tamaño
                    batch = []
                    current_batch_size = 0

            # Enviar el último batch si contiene líneas restantes
            if batch:
                data = Data(self.id, batch, dataset)
                sock.sendall(data.encode())
                # self.logger.custom(f"action: send_last_batch | result: success | dataset: {dataset} | batch_size: {current_batch_size} bytes")
            
            self.logger.custom(f"action: send_data | result: success | dataset: {dataset}")

    def _handle_sigterm(self, sig, frame):
        """Handle SIGTERM signal so the server closes gracefully."""
        self.logger.custom("Received SIGTERM, shutting down server.")
        self.shutting_down = True
        self._server_socket.close()
    
    def run(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
    
        try:
            self.client_socket.connect(self.server_addr)
            # Envía el mensaje Handshake
            handshake_msg = Handshake(self.id)  # Creamos el mensaje de tipo Handshake con ID 1
            self.client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_handshake | result: success | message: Handshake")
            
            self.send_dataset("/datasets/games-reducido.csv", self.client_socket, Dataset(Dataset.GAME))
            self.send_dataset("/datasets/reviews-reducido.csv", self.client_socket, Dataset(Dataset.REVIEW))
            
            # Envía el mensaje Fin
            fin_msg = Fin(self.id)  # Creamos el mensaje Fin con ID 1
            self.client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_fin | result: success | message: Fin")

            self.recv_results()
            


        # except Exception as error:
        #     if not self.shutting_down:
        #         self.logger.custom(f"Error: {error}")
        
        finally:
            self.client_socket.close()

    def recv_results(self):
            """Receive and process results from the server."""

            received_results = 0
            try:
                while received_results < 5:
                    # Recibe el mensaje del servidor
                    raw_msg = recv_msg(self.client_socket)
                    if not raw_msg:
                        print("Connection closed by server.")
                        break

                    # Decodifica el mensaje
                    msg = decode_msg(raw_msg)
                    
                    # Procesa el mensaje basado en su tipo
                    if msg.type == MsgType.RESULT:
                        received_results += 1
                        if msg.result_type == QueryNumber.Q1:
                            print(
                                f"Q1: OS Count Summary:\n"
                                f"Windows: {msg.windows_count}\n"
                                f"Linux: {msg.linux_count}\n"
                                f"Mac: {msg.mac_count}\n"
                            )
                            with open("/results/Q1.txt", "w") as file:
                                file.write(
                                    f"Q1: OS Count Summary:\n"
                                    f"Windows: {msg.windows_count}\n"
                                    f"Linux: {msg.linux_count}\n"
                                    f"Mac: {msg.mac_count}\n"
                                )
                        elif msg.result_type == QueryNumber.Q2:
                            top_games_str = "\n".join(f"- {name}: {playtime} average playtime" for name, playtime in msg.top_games)
                            print(
                                f"Q2: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
                                f"{top_games_str}\n"
                            )
                            with open("/results/Q2.txt", "w") as file:
                                file.write(
                                    f"Q2: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
                                    f"{top_games_str}\n"
                                )
                        elif msg.result_type == QueryNumber.Q3:
                            indie_games_str = "\n".join(f"{rank}. {name}: {reviews} positive reviews" 
                                                        for rank, (name, reviews) in enumerate(msg.top_indie_games, start=1))
                            print(
                                f"Q3: Top 5 Indie Games with Most Positive Reviews:\n"
                                f"{indie_games_str}\n"
                            )
                            with open("/results/Q3.txt", "w") as file:
                                file.write(
                                    f"Q3: Top 5 Indie Games with Most Positive Reviews:\n"
                                    f"{indie_games_str}\n"
                                )
                        elif msg.result_type == QueryNumber.Q4:
                            negative_reviews_str = "\n".join(f"- {name}: {count} negative reviews" for name, count in msg.negative_reviews)
                            print(
                                f"Q4: Action games with more than 5,000 negative reviews in English:\n"
                                f"{negative_reviews_str}\n"
                            )
                            with open("/results/Q4.txt", "w") as file:
                                file.write(
                                    f"Q4: Action games with more than 5,000 negative reviews in English:\n"
                                    f"{negative_reviews_str}\n"
                                )
                        elif msg.result_type == QueryNumber.Q5:
                            top_negative_str = "\n".join(f"- {name}: {count} negative reviews" for _, name, count in msg.top_negative_reviews)
                            print(
                                f"Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
                                f"{top_negative_str}\n"
                            )
                            with open("/results/Q5.txt", "w") as file:
                                file.write(
                                    f"Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
                                    f"{top_negative_str}\n"
                                )
                        else:
                            print(f"Received Unknown Result Type: {msg}")
                    else:
                        print(f"Received Non-Result Message: {msg}")
                    
            except Exception as e:
                print(f"Error receiving results: {e}")
