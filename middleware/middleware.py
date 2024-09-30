import pika
import logging

class Middleware:
    def __init__(self, host='rabbitmq'):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq')  # Asume que RabbitMQ está corriendo en el contenedor rabbitmq
        )
        self.channel = self.connection.channel()

        logging.info(f"action: init_middleware | result: success | host: {host}")
        self.queues = set()

    def declare_queue(self, queue_name):
        """
        Declara una nueva cola con el nombre proporcionado y la guarda.
        """
        if queue_name not in self.queues:
            self.channel.queue_declare(queue=queue_name)
            self.queues.add(queue_name)
            logging.info(f"action: declare_queue | result: success | queue_name: {queue_name}")
        else:
            logging.info(f"action: declare_queue | result: already_exists | queue_name: {queue_name}")

    def send_to_queue(self, queue_name, message):
        """
        Envía un mensaje a la cola especificada.
        """
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")
        
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,  # Cola a la que se envía el mensaje
                body=message
            )
            logging.info(f"action: send_to_queue | result: success | queue_name: {queue_name} | message: {message}")
        except Exception as e:
            logging.error(f"action: send_to_queue | result: fail | error: {e}")

    def receive_from_queue(self, queue_name):
        """
        Recibe un mensaje de la cola especificada y lo devuelve.
        """
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")
        
        # Obtén un solo mensaje de la cola
        method_frame, header_frame, body = self.channel.basic_get(queue=queue_name, auto_ack=True)
        
        if method_frame:
            logging.info(f"action: receive_from_queue | result: success | queue_name: {queue_name} | message: {body.decode()}")
            return body.decode()
        else:
            logging.info(f"action: receive_from_queue | result: no_message | queue_name: {queue_name}")
            return None

    def close(self):
        """
        Cierra la conexión a RabbitMQ.
        """
        self.connection.close()
        logging.info("action: close_connection | result: success")