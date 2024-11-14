import logging
import signal
from messages.messages import decode_msg
from middleware.middleware import Middleware




class ResultDispatcher:
    """Listens to RabbitMQ result queues and dispatches results to the correct client."""

    def __init__(self, client_connections, notification_queue, result_queue):
        self.client_connections = client_connections  # Shared dictionary with client_id -> socket
        self.notification_queue = notification_queue # Comunication queue with Server
        self.processes = []
        self.queue = result_queue
        self._middleware = Middleware()  # Each process gets its own Middleware instance
        self._middleware.declare_queue(self.queue)
        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, sig, frame):
        self._shutdown()

    def _shutdown(self):
        if self.shutting_down:
            return
        logging.info("action: Dispatcher shutdown | result: in progress...")
        self.shutting_down = True

        # Cierra la conexión de manera segura
        self._middleware.close()
        logging.info("action: Dispatcher shutdown | result: success")

    def listen_to_queue(self):
        """Listen to a specific queue and process messages as they arrive."""
        # handler de SIGTERM para procesos hijos
        try:
            # Blocking call to receive messages
            self._middleware.receive_from_queue(self.queue, self._process_result_callback, False)
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listen_to_queue | result: fail | error: {e}")
                self._shutdown()  # Trigger shutdown on error

    def _process_result_callback(self, ch, method, properties, body):
        """Callback to process messages from result queues."""
        try:
            result_msg = decode_msg(body[4:])

            client_id = result_msg.id  # Assuming result message has a client_id field

            # Find the socket associated with the client_id
            # Supuestamente le diccionario esta hecho con un manager para controlar el accesos concurrente
            if client_id in self.client_connections:
                client_sock, n_results_sent = self.client_connections[client_id]

                client_sock.sendall(body)
                # le añado uno a respuestas enviadas
                n_results_sent += 1
                self.client_connections[client_id] = (client_sock, n_results_sent) 
                if n_results_sent == 5:
                    self.notification_queue.put(client_id)
            else:
                # Raise an exception if there's no active connection for client_id
                raise Exception(f"No active connection for client_id {client_id}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (ValueError, Exception) as e:
            if not self.shutting_down:
                logging.error(f"Error processing message from {method.routing_key}: {e}")
                self._shutdown()  # Trigger shutdown on error
