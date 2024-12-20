import logging
import os
import signal
import socket

from messages.messages import ClientData, Dataset, MsgType, SimpleMessage, decode_msg
from messages.results_msg import QueryNumber
from utils.utils import recv_msg


class Client:

    def __init__(self, id: int, server_addr: tuple[str, id], max_batch_size, games, reviews):
        """
        Inicializa a la estructura interna del cliente: su estado, socket y signal handler.
        """
        self.id = id
        self.server_addr = server_addr
        self.max_batch_size = max_batch_size * 1024
        self.shutting_down = False
        self.games = games
        self.reviews = reviews
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
    
    def run(self):
        """
        Inicia la lógica del cliente.
        Se conecta al server, hace un handshake y envía primero el dataset de juegos y luego el de reviews.
        Termina con un mensaje FIN y luego espera los resultados.
        """

        try:
            self.client_socket.connect(self.server_addr)
            # Envía el mensaje Handshake
            handshake_msg = SimpleMessage(type=MsgType.HANDSHAKE, socket_compatible=True)  # Creamos el mensaje de tipo Handshake
            self.client_socket.send(handshake_msg.encode())  # Codificamos y enviamos el mensaje
            logging.info("action: send_handshake | result: success | message: Handshake")
            
            self.send_dataset(self.games, Dataset(Dataset.GAME))
            self.send_dataset(self.reviews, Dataset(Dataset.REVIEW))
            
            # Envía el mensaje Fin
            fin_msg = SimpleMessage(type=MsgType.CLIENT_FIN, socket_compatible=True)  # Creamos el mensaje Fin
            self.client_socket.send(fin_msg.encode())  # Codificamos y enviamos el mensaje
            logging.info("action: send_fin | result: success | message: Fin")

            self.recv_results()
            
        except Exception as error:
            if not self.shutting_down:
                logging.info(f"Error en run: {error}")

        finally:
            self.client_socket.close()

    def send_dataset(self, fname, dataset_type: Dataset):
        """
        Envía el dataset pasado por parámetro al server, con batches configurables.
        """
        # Chequear si existe el archivo
        with open(fname, mode='r') as file:
            next(file)  # Para saltearse el header
            batch = []
            current_batch_size = 0  # Tamaño actual del batch en bytes

            for line in file:
                line = line.strip()
                line_size = len(line.encode('utf-8'))  # Tamaño de la línea en bytes

                # Verifica si agregar esta línea excedería el tamaño del batch
                if current_batch_size + line_size > self.max_batch_size:
                    # Envía el batch actual y reinicia
                    data = ClientData(rows=batch, dataset=dataset_type)  # Usa el batch completo
                    self.client_socket.sendall(data.encode())  # Envía el batch codificado
                    # logging.info(f"action: send_batch | result: success | dataset: {dataset} | batch_size: {current_batch_size} bytes | lines: {len(batch)}")
                    
                    # Reinicia el batch y el contador de tamaño
                    batch = []
                    current_batch_size = 0

                # Agrega la línea actual al batch y actualiza el tamaño
                batch.append(line)
                current_batch_size += line_size

            # Enviar el último batch si contiene líneas restantes
            if batch:
                data = ClientData(rows=batch, dataset=dataset_type)
                self.client_socket.sendall(data.encode())
                # logging.info(f"action: send_last_batch | result: success | dataset: {dataset} | batch_size: {current_batch_size} bytes")
            
        logging.info(f"action: send_data | result: success | dataset: {dataset_type}")

    def _handle_sigterm(self, sig, frame):
        """
        Maneja señales SIGTERM para hacer un graceful shutdown del cliente.
        """
        logging.info("action: Received SIGTERM | shutting down server.")
        self.shutting_down = True
        self.client_socket.close()

    def recv_results(self):
        """
        Recibe y procesa la respuesta con los resultados del server.
        """

        logging.info(f"action: recv_results | result: in progress...")
        received_results = 0
        try:
            while received_results < 5:
                # Recibe el mensaje del servidor
                raw_msg = recv_msg(self.client_socket)
                if not raw_msg:
                    logging.error("Connection closed by server.")
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
                        logging.error(f"Received Unknown Result Type: {msg}")
                else:
                    logging.error(f"Received Non-Result Message: {msg}")

        except Exception as e:
            if not self.shutting_down:
                logging.error(f"Error receiving results: {e}")

    def save_to_file(self, filename: str, content: str, id: int):
        """
        Guarda el contenido de una query en un archivo especificado.
        """
        results_dir = f"/results/results_client_{id}"  # Define el directorio de resultados para cada cliente
        os.makedirs(results_dir, exist_ok=True)
        with open(f"{results_dir}/{filename}", "w") as file:
            file.write(content)
        
    def process_q1_result(self, msg):
        """
        Procesa, imprime y guarda en un archivo el resultado de Q1.
        """
        output = (
            f"\nQ1: OS Count Summary:\n"
            f"- Windows: {msg.result.windows_count}\n"
            f"- Linux: {msg.result.linux_count}\n"
            f"- Mac: {msg.result.mac_count}\n"
        )
        logging.info(output)
        self.save_to_file("Q1.txt", output, msg.client_id)

    def process_q2_result(self, msg):
        """
        Procesa, imprime y guarda en un archivo el resultado de Q2.
        """
        top_games_str = "\n".join(f"- {name}: {playtime} average playtime" for name, playtime in msg.result.top_games)
        output = (
            f"\nQ2: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:\n"
            f"{top_games_str}\n"
        )
        logging.info(output)
        self.save_to_file("Q2.txt", output, msg.client_id)

    def process_q3_result(self, msg):
        """
        Procesa, imprime y guarda en un archivo el resultado de Q3.
        """
        indie_games_str = "\n".join(
            f"{rank}. {name}: {reviews} positive reviews" for rank, (name, reviews) in enumerate(msg.result.top_indie_games, start=1)
        )
        output = (
            f"\nQ3: Top 5 Indie Games with Most Positive Reviews:\n"
            f"{indie_games_str}\n"
        )
        logging.info(output)
        self.save_to_file("Q3.txt", output, msg.client_id)

    def process_q4_result(self, msg):
        """
        Procesa, imprime y guarda en un archivo el resultado de Q4.
        """
        negative_reviews_str = "\n".join(f"- {name}: {count} negative reviews" for _, name, count in msg.result.negative_reviews)
        output = (
            f"\nQ4: Action games with more than 5,000 negative reviews in English:\n"
            f"{negative_reviews_str}\n"
        )
        logging.info(output)
        self.save_to_file("Q4.txt", output, msg.client_id)

    def process_q5_result(self, msg):
        """
        Procesa, imprime y guarda en un archivo el resultado de Q5.
        """
        top_negative_str = "\n".join(f"- {name}: {count} negative reviews" for _, name, count in msg.result.top_negative_reviews)
        output = (
            f"\nQ5: Games in the 90th Percentile for Negative Reviews (Action Genre):\n"
            f"{top_negative_str}\n"
        )
        logging.info(output)
        self.save_to_file("Q5.txt", output, msg.client_id)
