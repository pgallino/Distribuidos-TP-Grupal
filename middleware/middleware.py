import logging
import pika
import time
import utils.logging_config # Esto ejecuta la configuración del logger
from typing import List, Tuple, Callable


class Middleware:
    def __init__(self, host='rabbitmq'):
        """
        Inicializa la conexión con RabbitMQ y el canal.
        """
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.channel = None
        self.queues = set()
        self.exchanges = set()

        try:
            self.connection = self._connect_to_rabbitmq()
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
            self.logger.custom(f"action: middleware init_middleware | result: success | host: {host}")
        except Exception as e:
            raise Exception(f"action: middleware init_middleware | result: fail | error: {e}")

    def declare_queue(self, queue_name):
        """
        Declara una nueva cola con el nombre proporcionado y la guarda.
        """
        if queue_name not in self.queues:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queues.add(queue_name)
            self.logger.custom(f"action: middleware declare_queue | result: success | queue_name: {queue_name}")
        else:
            self.logger.error(f"action: middleware declare_queue | result: fail | queue_name: {queue_name} already exist")
    
    def declare_exchange(self, exchange, type='direct'):
        """
        Declara un nuevo exchange del tipo proporcionado e identificado con el nombre
        proporcionado, y lo guarda.
        """
        if exchange not in self.exchanges:
            self.channel.exchange_declare(exchange=exchange, exchange_type=type)
            self.exchanges.add(exchange)
            self.logger.custom(f"action: middleware declare_queue | result: success | exchange: {exchange}")
        else:
            self.logger.error(f"action: middleware declare_queue | result: fail | exchange: {exchange} already exist")
        
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
                raise Exception(f"action: middleware send_to_queue | result: fail | error: {e}")
        elif log in self.exchanges:
            try:
                self.channel.basic_publish(
                    exchange=log,
                    routing_key=key,
                    body=message
                )
            except Exception as e:
                raise Exception(f"action: middleware send_to_queue | result: fail | error: {e}")
        else:
            raise ValueError(f"La cola '{log}' no está declarada.")
    
    def receive_from_queue(self, queue_name, callback, auto_ack=True):
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")

        # Verifica si el canal está activo antes de configurarlo para el consumo
        if self.channel is None or self.channel.is_closed:
            raise RuntimeError("middleware: El canal no está disponible para consumir mensajes.")

        # Configura el consumidor en el canal con auto_ack
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        # Inicia el consumo de mensajes
        self.channel.start_consuming()
    
    def receive_from_queues(self, queues_with_callbacks: List[Tuple[str, Callable]], auto_ack=True):
        for queue_with_callback in queues_with_callbacks:
            queue_name, callback = queue_with_callback
            if queue_name not in self.queues:
                raise ValueError(f"La cola '{queue_with_callback[0]}' no está declarada.")
            
            # Verifica si el canal está activo antes de configurarlo para el consumo
            if self.channel is None or self.channel.is_closed:
                raise RuntimeError("middleware: El canal no está disponible para consumir mensajes.")
            
            # Configura el consumidor en el canal con auto_ack
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        # Inicia el consumo de mensajes
        self.channel.start_consuming()

    def close(self):
        """
        Cierra la conexión a RabbitMQ de forma segura.
        """
        if self.connection and not self.connection.is_closed:
            try:
                # Detiene el consumo antes de cerrar
                if self.channel and self.channel.is_open:
                    self.channel.stop_consuming()

                # # Asegurarse de que no hay callbacks pendientes
                if hasattr(self.channel, 'callbacks') and self.channel.callbacks:
                    self.channel.callbacks.clear()

                # Cerrar el canal y la conexión de forma segura
                if self.channel.is_open:
                    self.channel.close()
                if not self.connection.is_closed:
                    self.connection.close()
            except Exception as e:
                raise Exception(f"action: middleware close_connection | result: fail | error: {e}")
        else:
            raise Exception("action: middleware close_connection | result: fail | message: connection already closed or not initialized")

    def declare_queues(self, queues_list):
        """
        Declara un listado de colas
        """
        for queue in queues_list:
            self.declare_queue(queue)

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