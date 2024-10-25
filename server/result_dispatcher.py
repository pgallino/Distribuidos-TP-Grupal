import logging
from messages.messages import decode_msg
from middleware.middleware import Middleware




class ResultDispatcher:
    """Listens to RabbitMQ result queues and dispatches results to the correct client."""

    def __init__(self, client_connections, notification_queue, result_queue):
        self.logger = logging.getLogger(__name__)
        self.client_connections = client_connections  # Shared dictionary with client_id -> socket
        self.notification_queue = notification_queue # Comunication queue with Server
        self.processes = []
        self.queue = result_queue
        self._middleware = Middleware()  # Each process gets its own Middleware instance
        self._middleware.declare_queue(self.queue)
        self.shutting_down = False

    def _handle_sigterm(self, sig, frame):
        if self.shutting_down:
            return
        self.shutting_down = True
        self._middleware.close()


    def listen_to_queue(self):
        """Listen to a specific queue and process messages as they arrive."""
        # handler de SIGTERM para procesos hijos
        try:
            # Blocking call to receive messages
            self._middleware.receive_from_queue(self.queue, self._process_result_callback, False)
        finally:
            # Ensure middleware is closed properly
            self._middleware.close()

    def _process_result_callback(self, ch, method, properties, body):
        """Callback to process messages from result queues."""
        try:
            result_msg = decode_msg(body)

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
                self.logger.error(f"No active connection for client_id {client_id}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except ValueError as e:
            self.logger.custom(f"Error decoding message from {method.routing_key}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to process message from {method.routing_key}: {e}")
