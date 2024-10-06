import logging
import pika
import time
import utils.logging_config # Esto ejecuta la configuración del logger


class Middleware:
    def __init__(self, host='rabbitmq'):

        self.logger = logging.getLogger(__name__)

        self.connection = self._connect_to_rabbitmq()
        self.channel = self.connection.channel()
        
        self.logger.custom(f"action: init_middleware | result: success | host: {host}")
        self.queues = set()
        self.exchanges = set()

    def declare_queue(self, queue_name):
        """
        Declara una nueva cola con el nombre proporcionado y la guarda.
        """
        if queue_name not in self.queues:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queues.add(queue_name)
            self.logger.custom(f"action: declare_queue | result: success | queue_name: {queue_name}")
        else:
            self.logger.custom(f"action: declare_queue | result: fail | queue_name: {queue_name} already exist")
    
    def declare_exchange(self, exchange, type='direct'):
        """
        Declara un nuevo exchange del tipo proporcionado e identificado con el nombre
        proporcionado, y lo guarda.
        """
        if exchange not in self.queues:
            self.channel.exchange_declare(exchange=exchange, exchange_type=type)
            self.exchanges.add(exchange)
            self.logger.custom(f"action: declare_queue | result: success | exchange: {exchange}")
        else:
            self.logger.custom(f"action: declare_queue | result: fail | exchange: {exchange} already exist")
        
    def bind_queue(self, queue_name, exchange, key=None):
        if queue_name not in self.queues or exchange not in self.exchanges:
            raise ValueError(f"No se encontro la cola {queue_name} o el exchange {exchange}.")
        self.channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=key)

    def send_to_queue(self, log, message, key=''):
        """
        Envía un mensaje a la cola especificada.
        """
        if log in self.queues:
            try:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=log,  # Cola a la que se envía el mensaje
                    body=message
                )
            except Exception as e:
                logging.error(f"action: send_to_queue | result: fail | error: {e}")
        elif log in self.exchanges:
            try:
                self.channel.basic_publish(
                    exchange=log,
                    routing_key=key,
                    body=message
                )
            except Exception as e:
                logging.error(f"action: send_to_queue | result: fail | error: {e}")
        else:
            raise ValueError(f"La cola '{log}' no está declarada.")
        

    def receive_from_queue(self, queue_name, block=True):
        """
        Recibe un mensaje de la cola especificada y lo devuelve.
        Si no hay mensaje y 'block' es True, esperará hasta que llegue un mensaje.
        """
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")
        
        while True:
            # Obtén un solo mensaje de la cola
            method_frame, header_frame, body = self.channel.basic_get(queue=queue_name, auto_ack=True)
            
            if method_frame:
                return body
            else:
                if not block:
                    self.logger.custom(f"action: receive_from_queue | result: no_message | queue_name: {queue_name}")
                    return None
                time.sleep(5)  # Espera activa de 1 segundo antes de revisar de nuevo

    def close(self):
        """
        Cierra la conexión a RabbitMQ.
        """
        self.connection.close()
        self.logger.custom("action: close_connection | result: success")

    def _connect_to_rabbitmq(self):
        retries = 5
        for i in range(retries):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters('rabbitmq')
                )
                return connection
            except pika.exceptions.AMQPConnectionError:
                logging.error(f"Intento {i+1} de {retries}: No se puede conectar a RabbitMQ. Reintentando...")
                time.sleep(5)
        raise Exception("No se pudo conectar a RabbitMQ después de varios intentos.")