import logging
import os
import signal
import socket

from messages.messages import Data, Dataset, Fin, Handshake, MsgType, decode_msg
from messages.results_msg import QueryNumber
from utils.utils import recv_msg


class Client:

    def __init__(self, id: int, server_addr: tuple[str, id], max_batch_size, games, reviews):
        self.id = id
        self.server_addr = server_addr
        self.max_batch_size = max_batch_size * 1024
        self.logger = logging.getLogger(__name__)
        self.shutting_down = False
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.games = games
        self.reviews = reviews
        signal.signal(signal.SIGTERM, self._handle_sigterm)
    
    def run(self):
    
        try:
            self.client_socket.connect(self.server_addr)
            # Envía el mensaje Handshake
            handshake_msg = Handshake(self.id)  # Creamos el mensaje de tipo Handshake con ID 1
            self.client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_handshake | result: success | message: Handshake")
            
            self.send_dataset(self.games, self.client_socket, Dataset(Dataset.GAME))
            self.send_dataset(self.reviews, self.client_socket, Dataset(Dataset.REVIEW))
            
            # Envía el mensaje Fin
            fin_msg = Fin(self.id)  # Creamos el mensaje Fin con ID 1
            self.client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
            self.logger.custom("action: send_fin | result: success | message: Fin")

            self.recv_results()
            
        except Exception as error:
            if not self.shutting_down:
                self.logger.custom(f"Error: {error}")
        
        finally:
            self.client_socket.close()

    def send_dataset(self, fname, sock, dataset):
        # Chequear si existe el archivo
        with open(fname, mode='r') as file:
            self.logger.custom(f"action: send_handshake | result: success | dataset: {dataset}")
            next(file)  # Para saltearse el header
            batch = []
            current_batch_size = 0  # Tamaño actual del batch en bytes

            for line in file:
                line = line.strip()
                line_size = len(line.encode('utf-8'))  # Tamaño de la línea en bytes

                # Verifica si agregar esta línea excedería el tamaño del batch
                if current_batch_size + line_size > self.max_batch_size:
                    # Envía el batch actual y reinicia
                    data = Data(self.id, batch, dataset)  # Usa el batch completo
                    sock.sendall(data.encode())  # Envía el batch codificado
                    # self.logger.custom(f"action: send_batch | result: success | dataset: {dataset} | batch_size: {current_batch_size} bytes | lines: {len(batch)}")
                    
                    # Reinicia el batch y el contador de tamaño
                    batch = []
                    current_batch_size = 0

                # Agrega la línea actual al batch y actualiza el tamaño
                batch.append(line)
                current_batch_size += line_size

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

    def recv_results(self):
        """Receive and process results from the server."""

        self.logger.custom(f"action: recv_results | result: in progress...")
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
                    # Llama al método correspondiente basado en el tipo de resultado
                    if msg.result_type == QueryNumber.Q1:
                        self.process_q1_result(msg)
                    elif msg.result_type == QueryNumber.Q2:
                        self.process_q2_result(msg)
                    elif msg.result_type == QueryNumber.Q3:
                        self.process_q3_result(msg)
                    elif msg.result_type == QueryNumber.Q4:
                        self.process_q4_result(msg)
                    elif msg.result_type == QueryNumber.Q5:
                        self.process_q5_result(msg)
                    else:
                        print(f"Received Unknown Result Type: {msg}")
                else:
                    print(f"Received Non-Result Message: {msg}")

        except Exception as e:
            print(f"Error receiving results: {e}")

    def save_to_file(self, filename: str, content: str, id: int):
        """Saves the content to a specified file."""
        results_dir = f"/results/results_client_{id}"  # Define el directorio de resultados para cada cliente
        os.makedirs(results_dir, exist_ok=True)
        with open(f"{results_dir}/{filename}", "w") as file:
            file.write(content)
        
    def process_q1_result(self, msg):
        """Processes and prints Q1 result."""
        output = (
            f"Q1: OS Count Summary:\n"
            f"- Windows: {msg.windows_count}\n"
            f"- Linux: {msg.linux_count}\n"
            f"- Mac: {msg.mac_count}\n"
        )
        self.logger.custom(output)
        self.save_to_file("Q1.txt", output, msg.id)

    def process_q2_result(self, msg):
        """Processes and prints Q2 result."""
        top_games_str = "\n".join(f"- {name}: {playtime} average playtime" for name, playtime in msg.top_games)
        output = (
            f"Q2: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
            f"{top_games_str}\n"
        )
        self.logger.custom(output)
        self.save_to_file("Q2.txt", output, msg.id)

    def process_q3_result(self, msg):
        """Processes and prints Q3 result."""
        indie_games_str = "\n".join(
            f"{rank}. {name}: {reviews} positive reviews" for rank, (name, reviews) in enumerate(msg.top_indie_games, start=1)
        )
        output = (
            f"Q3: Top 5 Indie Games with Most Positive Reviews:\n"
            f"{indie_games_str}\n"
        )
        self.logger.custom(output)
        self.save_to_file("Q3.txt", output, msg.id)

    def process_q4_result(self, msg):
        """Processes and prints Q4 result."""
        negative_reviews_str = "\n".join(f"- {name}: {count} negative reviews" for _, name, count in msg.negative_reviews)
        output = (
            f"Q4: Action games with more than 5,000 negative reviews in English:\n"
            f"{negative_reviews_str}\n"
        )
        self.logger.custom(output)
        self.save_to_file("Q4.txt", output, msg.id)

    def process_q5_result(self, msg):
        """Processes and prints Q5 result."""
        top_negative_str = "\n".join(f"- {name}: {count} negative reviews" for _, name, count in msg.top_negative_reviews)
        output = (
            f"Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
            f"{top_negative_str}\n"
        )
        self.logger.custom(output)
        self.save_to_file("Q5.txt", output, msg.id)
