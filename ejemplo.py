class CoordinatorNode:

    def __init__(self, id: int, n_nodes: int, exchange: str) -> None:
        self.id = id
        self._middleware = Middleware()
        self.n_nodes = n_nodes
        self.exchange = exchange
        self.general_queue = f"general_queue_{self.id}"  # Cada nodo tiene su propia cola general
        self._setup_queue()

    def _setup_queue(self):
        """Configura la cola para mensajes generales y mensajes con routing keys específicas."""
        # Declara el exchange de tipo 'topic'
        self._middleware.declare_exchange(self.exchange, exchange_type='topic')

        # Declara la cola general del nodo
        self._middleware.declare_queue(self.general_queue)

        # Bind para que el nodo reciba todos los mensajes generales
        self._middleware.bind_queue(self.general_queue, self.exchange, routing_key="general.*")

        # Bind para que el nodo reciba mensajes FIN específicos para su ID
        specific_fin_key = f"node_{self.id}.fin"
        self._middleware.bind_queue(self.general_queue, self.exchange, routing_key=specific_fin_key)

    def _listen_queue(self):
        """Escucha la cola de este nodo."""
        self._middleware.receive_from_queue(self.general_queue, self.process_message, auto_ack=False)

    def process_message(self, ch, method, properties, raw_message):
        """Procesa los mensajes recibidos."""
        msg = decode_msg(raw_message)

        if msg.type == MsgType.DATA:
            self._process_data_message(msg)
        elif msg.type == MsgType.FIN:
            self._process_fin_message(msg)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_data_message(self, msg):
        """Procesa los mensajes de tipo DATA."""
        # Código de procesamiento de datos
        pass

    def _process_fin_message(self, msg):
        """Procesa los mensajes de tipo FIN."""
        # Código de procesamiento FIN específico para este nodo
        pass

# Ejemplo de envío de mensajes:

# Enviar un mensaje general a todos los nodos
middleware.send_to_exchange(exchange_name, "general.data", msg.encode())

# Enviar un mensaje FIN específicamente al nodo 2
middleware.send_to_exchange(exchange_name, "node_2.fin", msg_fin.encode())

# ================================================================================================================
    def __init__(self, id: int, n_nodes: int, exchange: str) -> None:
        self.id = id
        self._middleware = Middleware()
        self.n_nodes = n_nodes
        self.exchange = exchange
        self.general_queue = "shared_general_queue"  # Todos los nodos compartirán esta cola
        self._setup_queue()

    def _setup_queue(self):
        """Configura la cola para mensajes generales compartidos y mensajes con routing keys específicas."""
        # Declara el exchange de tipo 'topic'
        self._middleware.declare_exchange(self.exchange, exchange_type='topic')

        # Declara una única cola general compartida para todos los nodos
        self._middleware.declare_queue(self.general_queue)

        # Bind para que la cola general reciba todos los mensajes generales (round-robin entre nodos)
        self._middleware.bind_queue(self.general_queue, self.exchange, routing_key="general.*")

        # Cada nodo sigue teniendo un binding para los mensajes FIN específicos para su ID
        specific_fin_key = f"node_{self.id}.fin"
        self._middleware.declare_queue(f"fin_queue_{self.id}")
        self._middleware.bind_queue(f"fin_queue_{self.id}", self.exchange, routing_key=specific_fin_key)

# ================================================================================================
import logging
from messages.messages import MsgType, decode_msg
from middleware.middleware import Middleware
from utils.constants import Q_GATEWAY_TRIMMER
from utils.utils import recv_msg


class ConnectionHandler:
    """Handles communication with a connected client in a separate process."""

    def __init__(self, id, client_sock, n_next_nodes, exchange_name):
        self.id = id
        self.client_sock = client_sock
        self.n_next_nodes = n_next_nodes
        self.exchange_name = exchange_name
        self._middleware = Middleware()  # Cada proceso hijo tiene su propia conexión de middleware
        self._middleware.declare_exchange(exchange_name, exchange_type='topic')
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Ejecuta la lógica principal para manejar la conexión de un cliente."""
        try:
            while True:
                raw_msg = recv_msg(self.client_sock)
                msg = decode_msg(raw_msg)
                msg.id = self.id  # Actualiza el ID del mensaje con el ID del nodo

                # Procesar el mensaje según su tipo
                if msg.type == MsgType.DATA:
                    # Enviar mensaje de tipo DATA a todos los nodos (clave "general.data")
                    routing_key = "general.data"
                    self._middleware.send_to_exchange(self.exchange_name, routing_key, msg.encode())
                elif msg.type == MsgType.FIN:
                    # Enviar mensaje de tipo FIN a nodos específicos con su clave ("node_{id}.fin")
                    for node_id in range(1, self.n_next_nodes + 1):
                        routing_key = f"node_{node_id}.fin"
                        self._middleware.send_to_exchange(self.exchange_name, routing_key, msg.encode())
                    break  # Finaliza cuando se envía FIN

        except ValueError as e:
            self.logger.custom(f"Connection closed or invalid message received: {e}")
        except OSError as e:
            self.logger.error(f"action: receive_message | result: fail | error: {e}")
        except Exception as e:
            self.logger.error(f"action: listen_to_queue | result: fail | error: {e}")
        finally:
            # Clean up
            self._middleware.close()
