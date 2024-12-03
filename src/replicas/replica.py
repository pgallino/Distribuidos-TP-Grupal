import logging
from multiprocessing import Process
import signal
import socket
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.middleware_constants import E_FROM_MASTER_PUSH, E_FROM_REPLICA_PULL, Q_MASTER_REPLICA, Q_REPLICA_MASTER
from utils.container_constants import LISTENER_PORT
from utils.listener import ReplicaListener
from utils.utils import simulate_random_failure, log_with_location

class Replica:
    def __init__(self, id: int, ip_prefix: str, container_to_restart: str):
        self.id = id
        self.shutting_down = False
        self._middleware = Middleware()
        self.container_to_restart = container_to_restart
        self.state = None
        self.ip_prefix = ip_prefix
        self.port = LISTENER_PORT
        self.sincronizado = False
        # Manejo de señales
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self._initialize_storage()

        if not ip_prefix:
            raise ValueError("ip_prefix no puede ser None o vacío.")
        if not container_to_restart:
            raise ValueError("container_to_restart no puede ser None o vacío.")

        self.recv_queue = Q_MASTER_REPLICA + f"_{ip_prefix}_{self.id}"
        self.send_queue = E_FROM_REPLICA_PULL
        self.exchange_name = E_FROM_MASTER_PUSH + f"_{container_to_restart}"
        self._middleware.declare_queue(self.recv_queue) # -> cola por donde recibo pull y push
        self._middleware.declare_exchange(self.exchange_name, type = "fanout")
        self._middleware.bind_queue(self.recv_queue, self.exchange_name) # -> bindeo al fanout de los push y pull
        self._middleware.declare_exchange(E_FROM_REPLICA_PULL)
        self.sync_exchange = "E_SYNC_STATE"
        self._middleware.declare_exchange(self.sync_exchange)
        
        self.listener = Process(target=init_listener, args=(id, ip_prefix, self.port,))
        self.listener.start()

    def run(self):
        """Inicia el consumo de mensajes en la cola de la réplica."""
        try:
            while not self.shutting_down:
                # AHORA REVIVE EL WATCHDOG A LOS MASTERS, NO NECESITO VERIFICAR CON TIMEOUT
                self._middleware.receive_from_queue(self.recv_queue, self.process_replica_message, auto_ack=False)
                self.recover_state()  # Método para solicitar sincronización
        except Exception as e:
            if not self.shutting_down:
                logging.error(f"action: listening_queue | result: fail | error: {e.with_traceback()}")
                self._shutdown()

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas."""
        pass

    def _process_push_data(self, msg):
        """Procesa los datos de un mensaje de `PushDataMessage`."""
        pass

    def _load_state(self, msg):
        pass

    def _process_pull_data(self):
        """Procesa un mensaje de solicitud de pull de datos."""

        # ==================================================================
        # CAIDA PROCESANDO PULL_DATA ANTES DE ENVIAR RESPUESTA
        simulate_random_failure(self, log_with_location("CAIDA PROCESANDO PULL_DATA ANTES DE ENVIAR RESPUESTA"))
        # ==================================================================

        if self.last_msg_id == 0:
            self._middleware.send_to_queue(self.send_queue, SimpleMessage(type=MsgType.EMPTY_STATE, node_id = self.id).encode())
            logging.info("Replica: Estado empty enviado en respuesta a PullDataMessage.")
            return
        
        self._middleware.send_to_queue(self.send_queue, self._create_pull_answer().encode())
        logging.info("Replica: Estado completo enviado en respuesta a PullDataMessage.")

        # ==================================================================
        # CAIDA PROCESANDO PULL_DATA LUEGO DE ENVIAR RESPUESTA
        simulate_random_failure(self, log_with_location("CAIDA PROCESANDO PULL_DATA LUEGO DE ENVIAR RESPUESTA"))
        # ==================================================================

    def _create_pull_answer(self):
        pass

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return

        logging.info("action: shutdown_replica | result: in progress...")
        self.shutting_down = True

        if self.listener:
            self.listener.terminate()
            self.listener.join()

        try:
            self._middleware.close()
            logging.info("action: shutdown_replica | result: success")
        except Exception as e:
            logging.error(f"action: shutdown_replica | result: fail | error: {e}")

        self._middleware.check_closed()
        exit(0)

    def _handle_sigterm(self, sig, frame):
        """Maneja la señal SIGTERM para cerrar la réplica de forma segura."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()

    def process_replica_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_REPLICA`."""
        try:
            # ==================================================================
            # CAIDA LUEGO DE CONSUMIR MENSAJE Y ANTES DE DAR EL ACK
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE CONSUMIR MENSAJE Y ANTES DE DAR EL ACK"))
            # ==================================================================
            msg = decode_msg(raw_message)

            # Determinar si la réplica necesita sincronización
            if msg.msg_id > 0 and not self.sincronizado:
                # devuelvo el mensaje a la cola (si el msg_id es > 0 se trata de un push)
                ch.basic_nack(delivery_tag=method.delivery_tag)

                # ==================================================================
                # CAIDA LUEGO DE DEVOLVER A LA COLA CON NACK
                simulate_random_failure(self, log_with_location("CAIDA LUEGO DE DEVOLVER A LA COLA CON NACK"))
                # ==================================================================

                ch.stop_consuming()
                logging.info(f"Replica {self.id}: Recibiendo primer mensaje con ID {msg.msg_id} de tipo {msg.type}. Iniciando sincronización.")
                return # ya me sincronicé y me vuelvo a consumir por la cola principal
            
            if msg.type == MsgType.PULL_DATA: # si es un pull respondo siempre, si no estoy sincronizado se responde un Empty y listo
                self._process_pull_data()

            elif msg.type == MsgType.SYNC_STATE_REQUEST: # Procesar siempre los mensajes SYNC_STATE_REQUEST
                if msg.requester_id != self.id:  # Ignorar solicitudes propias
                    if self.sincronizado: # respondo si se que estoy sincronizado
                        self._process_sync_state_request(msg.requester_id)

            elif msg.type == MsgType.PUSH_DATA:
                # Procesar solo mensajes con un ID mayor al último procesado
                if msg.msg_id > self.last_msg_id or self.last_msg_id == 0:
                    self.last_msg_id = msg.msg_id
                    self.sincronizado = True
                    self._process_push_data(msg)

                    # ==================================================================
                    # CAIDA POST PROCESAR MENSAJE PUSH Y ANTES DE DAR EL ACK
                    simulate_random_failure(self, log_with_location("CAIDA POST PROCESAR MENSAJE PUSH Y ANTES DE DAR EL ACK"))
                    # ==================================================================

            # Confirmar la recepción del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # ==================================================================
            # CAIDA ACA POST PROCESAR MENSAJE Y DESPUES DE DAR EL ACK
            simulate_random_failure(self, log_with_location("CAIDA ACA POST PROCESAR MENSAJE Y DESPUES DE DAR EL ACK"))
            # ==================================================================

        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e}")
        
    def simulate_failure(self, id):
        """Simula la caída del id"""

        if self.id == id:
            self._shutdown()
            exit(0)
    
    def recover_state(self):
        """
        Solicita el estado a las réplicas compañeras y se sincroniza.
        """
        # logging.info(f"Replica {self.id}: Solicitando estado a réplicas compañeras.")

        # ==================================================================
        # CAIDA LUEGO DE ENTRAR A RECOVER_STATE Y ANTES DE ENVIAR SYNC_MSG
        simulate_random_failure(self, log_with_location("CAIDA LUEGO DE ENTRAR A RECOVER_STATE Y ANTES DE ENVIAR SYNC_MSG"))
        # ==================================================================

        # Crear una cola anónima y vincularla al exchange E_SYNC_STATE con la routing key basada en el ID de la réplica
        self.response_queue = self._middleware.declare_anonymous_queue(exchange_name=self.sync_exchange, routing_key=str(self.id))

        # Enviar Sync_state
        sync_msg = SimpleMessage(type=MsgType.SYNC_STATE_REQUEST, requester_id=self.id)
        # Lo envio a la cola de la que reciben todos
        self._middleware.send_to_queue(self.exchange_name, sync_msg.encode())

        # ==================================================================
        # CAIDA LUEGO DE ENVIAR SYNC_MSG Y ANTES DE ESPERAR RESPUESTA
        simulate_random_failure(self, log_with_location("CAIDA LUEGO DE ENVIAR SYNC_MSG Y ANTES DE ESPERAR RESPUESTA"))
        # ==================================================================

        # Esperar respuesta
        def on_state_response(ch, method, properties, body):
            msg = decode_msg(body)
            if isinstance(msg, PushDataMessage):
                self._load_state(msg)
                # logging.info(f"Replica {self.id}: Estado recuperado de la réplica compañera.")

            # ==================================================================
            # CAIDA LUEGO DE HACER LOAD Y ANTES DE DAR ACK AL SYNC_MSG
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE HACER LOAD Y ANTES DE DAR ACK AL SYNC_MSG"))
            # ==================================================================

            ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.stop_consuming()  # Terminar el consumo después de recibir una respuesta

            # ==================================================================
            # CAIDA LUEGO DE HACER LOAD Y LUEGO DE DAR ACK AL SYNC_MSG
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE HACER LOAD Y LUEGO DE DAR ACK AL SYNC_MSG"))
            # ==================================================================

        self._middleware.receive_from_queue(self.response_queue, on_state_response, auto_ack=False)
        self.sincronizado = True

    def _process_sync_state_request(self, requester_id):
        """
        Responde al mensaje SYNC_STATE_REQUEST enviado por otra réplica.
        Envía el estado actual al ID de la réplica que realizó la solicitud.
        """
        try:
            logging.info(f"Replica {self.id}: Respondiendo estado a la réplica {requester_id}.")

            # ==================================================================
            # CAIDA LUEGO DE RECIBIR SYNC_MSG Y ANTES DE ENVIARLO
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE RECIBIR SYNC_MSG Y ANTES DE ENVIARLO"))
            # ==================================================================
            
            # Crear el mensaje de respuesta con el estado actual
            response_data = self._create_pull_answer()
            logging.info(f"envio esta data en la sincro: {response_data}")

            # Publicar el estado en el exchange con la routing key del solicitante
            self._middleware.send_to_queue(
                self.sync_exchange,
                response_data.encode(),
                str(requester_id)
            )

            # ==================================================================
            # CAIDA LUEGO DE RECIBIR SYNC_MSG Y LUEGO DE ENVIARLO
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE RECIBIR SYNC_MSG Y LUEGO DE ENVIARLO"))
            # ==================================================================

            logging.info(f"Replica {self.id}: Estado enviado a la réplica {requester_id}.")
        except Exception as e:
            logging.error(f"Replica {self.id}: Error al procesar SYNC_STATE_REQUEST: {e}")

def init_listener(id, ip_prefix, port):
    listener = ReplicaListener(id, ip_prefix, port)
    listener.run()