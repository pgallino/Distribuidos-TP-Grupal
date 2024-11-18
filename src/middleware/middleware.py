import logging
import pika
import time
from typing import List, Tuple, Callable


class Middleware:
    def __init__(self, host='rabbitmq'):
        """
        Inicializa la conexión con RabbitMQ y el canal.
        """
        self.connection = None
        self.channel = None
        self.queues = set()
        self.exchanges = set()

        try:
            self.connection = self._connect_to_rabbitmq()
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
            logging.info(f"action: middleware init_middleware | result: success | host: {host}")
        except Exception as e:
            raise Exception(f"action: middleware init_middleware | result: fail | error: {e}")

    def declare_queue(self, queue_name):
        """
        Declara una nueva cola con el nombre proporcionado y la guarda.
        """
        if queue_name not in self.queues:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queues.add(queue_name)
            logging.info(f"action: middleware declare_queue | result: success | queue_name: {queue_name}")
        else:
            logging.error(f"action: middleware declare_queue | result: fail | queue_name: {queue_name} already exist")
    
    def declare_exchange(self, exchange, type='direct'):
        """
        Declara un nuevo exchange del tipo proporcionado e identificado con el nombre
        proporcionado, y lo guarda.
        """
        if exchange not in self.exchanges:
            self.channel.exchange_declare(exchange=exchange, exchange_type=type)
            self.exchanges.add(exchange)
            logging.info(f"action: middleware declare_queue | result: success | exchange: {exchange}")
        else:
            logging.error(f"action: middleware declare_queue | result: fail | exchange: {exchange} already exist")
        
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
    
    def receive_from_queue(self, queue_name, callback, auto_ack=True, get_blocked=True):
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")

        # Verifica si el canal está activo antes de configurarlo para el consumo
        if self.channel is None or self.channel.is_closed:
            raise RuntimeError("middleware: El canal no está disponible para consumir mensajes.")

        # Configura el consumidor en el canal con auto_ack
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        # Inicia el consumo de mensajes
        if get_blocked:
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

    def receive_from_queue_with_timeout(self, queue_name, callback, inactivity_time, auto_ack=True):
        if queue_name not in self.queues:
            raise ValueError(f"La cola '{queue_name}' no está declarada.")

        # Verifica si el canal está activo antes de configurarlo para el consumo
        if self.channel is None or self.channel.is_closed:
            raise RuntimeError("middleware: El canal no está disponible para consumir mensajes.")
        
        last_message_time = time.time()

        # Configura el consumidor en el canal con auto_ack
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        try:
            logging.info("Esperando mensajes. Presiona CTRL+C para salir.")
            while True:
                self.connection.process_data_events(time_limit=inactivity_time)  # Procesa eventos con un timeout corto
                current_time = time.time()
                
                # Verifica si se excedió el tiempo de inactividad
                if current_time - last_message_time > inactivity_time:
                    logging.info(f"Tiempo de inactividad excedido: {inactivity_time} segundos.")
                    break
        except KeyboardInterrupt:
            logging.info("Consumo interrumpido manualmente.")

    def close(self):
        """
        Cierra la conexión a RabbitMQ de forma segura.
        """
        logging.info("action: middleware close | status: start | message: Starting close process")
        if self.connection and not self.connection.is_closed:
            try:
                # Detiene el consumo antes de cerrar
                if self.channel and self.channel.is_open:
                    logging.info("action: middleware close | step: stop_consuming | status: in_progress")
                    try:
                        self.channel.stop_consuming()
                        logging.info("action: middleware close | step: stop_consuming | status: success")
                    except Exception as e:
                        logging.error(f"action: middleware close | step: stop_consuming | status: fail | error: {e}")
                        raise

                # Asegurarse de que no hay callbacks pendientes
                if hasattr(self.channel, 'callbacks') and self.channel.callbacks:
                    logging.info("action: middleware close | step: clear_callbacks | status: in_progress")
                    self.channel.callbacks.clear()
                    logging.info("action: middleware close | step: clear_callbacks | status: success")

                # Cerrar el canal de forma segura
                if self.channel.is_open:
                    logging.info("action: middleware close | step: close_channel | status: in_progress")
                    try:
                        self.channel.close()
                        logging.info("action: middleware close | step: close_channel | status: success")
                    except Exception as e:
                        logging.error(f"action: middleware close | step: close_channel | status: fail | error: {e}")
                        raise

                # Cerrar la conexión de forma segura
                if not self.connection.is_closed:
                    logging.info("action: middleware close | step: close_connection | status: in_progress")
                    try:
                        self.connection.close()
                        logging.info("action: middleware close | step: close_connection | status: success")
                    except Exception as e:
                        logging.error(f"action: middleware close | step: close_connection | status: fail | error: {e}")
                        raise

            except Exception as e:
                logging.error(f"action: middleware close | status: fail | error: {e}")
                raise
        else:
            logging.warning("action: middleware close | status: skipped | message: Connection already closed or not initialized")
        logging.info("action: middleware close | status: completed")

    def check_closed(self):
        """
        Verifica si el canal y la conexión están cerrados correctamente.
        """
        channel_status = "closed" if self.channel is None or self.channel.is_closed else "open"
        connection_status = "closed" if self.connection is None or self.connection.is_closed else "open"

        if channel_status == "closed" and connection_status == "closed":
            logging.info(f"action: middleware check_closed | channel_status: {channel_status} | connection_status: {connection_status} ✅ ")
        else:
            logging.error(f"action: middleware check_closed | channel_status: {channel_status} | connection_status: {connection_status} ❌")

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