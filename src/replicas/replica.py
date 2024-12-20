import logging
from multiprocessing import Process
import signal
import threading
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from utils.middleware_constants import E_FROM_MASTER_PUSH, E_FROM_REPLICA_PULL_ANS, E_REPLICA_SYNC_REQUEST_LISTENER, E_SYNC_STATE, Q_MASTER_REPLICA, Q_REPLICA_SYNC_REQUEST_LISTENER
from utils.container_constants import LISTENER_PORT, REPLICAS_PROB_FAILURE
from listener import Listener
from utils.utils import simulate_random_failure, log_with_location

class Replica:
    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        self.id = id
        self.n_replicas = n_replicas
        self.shutting_down = False
        self._middleware = Middleware()
        self.master_name = master_name
        self.sync_listener_process = None
        self.listener = None
        self.container_name = container_name
        self.port = LISTENER_PORT
        self.master_name = master_name
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.synchronized = False
        self.last_msg_id = 0

        # Lock para proteger el acceso al estado compartido
        self.lock = threading.Lock()

        # Validación de nombres de contenedor
        if not container_name:
            raise ValueError("container_name no puede ser None o vacío.")
        if not master_name:
            raise ValueError("master_name no puede ser None o vacío.")

        # Configuración de colas y exchanges

        # DE ESTE EXCHANGE RECIBO PUSHS Y FINS DEL MASTER
        self.recv_exchange = E_FROM_MASTER_PUSH + f"_{master_name}"
        self._middleware.declare_exchange(self.recv_exchange, type="fanout")

        # DE ESTA COLA RECIBO LOS PUSHS Y FINS DEL MASTER -> LA BINDEO AL EXCHANGE E_FROM_MASTER_PUSH
        self.recv_queue = Q_MASTER_REPLICA + f"_{container_name}_{self.id}"
        self._middleware.declare_queue(self.recv_queue)
        self._middleware.bind_queue(self.recv_queue, self.recv_exchange)

        # A ESTE EXCHANGE ENVIO LOS SYNC_STATE_REQUEST
        self.sync_request_listener_exchange = E_REPLICA_SYNC_REQUEST_LISTENER + f"_{master_name}"
        self._middleware.declare_exchange(self.sync_request_listener_exchange)

        # ESTA COLA LA UTILIZA EL THREAD SYNC_REQUEST_LISTENER PARA PROCESAR LAS SYNC_STATE_REQUEST EN PARALELO
        self.sync_request_listener_queue = Q_REPLICA_SYNC_REQUEST_LISTENER + f"_{master_name}_{self.id}"

        # EN ESTE EXCHANGE RECIBO LAS RESPUESTAS A MIS SYNC_STATE_REQUEST CON ESTADOS DE OTRAS REPLICAS -> LUEGO ME BINDEO CON UNA COLA ANONIMA PARA RECIBIR DE EL.
        self.sync_exchange = E_SYNC_STATE + f'_{container_name}'
        self._middleware.declare_exchange(self.sync_exchange, type='fanout')

        self._initialize_storage()

        # Hilo para manejar solicitudes de sincronización
        self.sync_listener_thread = threading.Thread(
            target=self._run_sync_listener,
            daemon=True
        )
        self.sync_listener_thread.start()

        self.listener = Process(target=init_listener, args=(id, container_name, self.port,))
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

    def _process_fin_message(self, msg):
        pass

    def _create_pull_answer(self):
        pass

    def _shutdown(self):
        """Cierra la réplica de forma segura."""
        if self.shutting_down:
            return


        logging.info("action: shutdown_replica | result: in progress...")
        self.shutting_down = True

        self._middleware.send_to_queue(self.sync_request_listener_exchange, SimpleMessage(type=MsgType.CLOSE).encode(), str(self.id))

        # Detener y unir el hilo de sincronización si está en ejecución
        if self.sync_listener_process and self.sync_listener_thread.is_alive():
            logging.info("action: shutdown_replica | stopping sync listener")
            self.sync_listener_thread.join()

        if self.listener:
            self.listener.terminate()
            self.listener.join()

        try:
            self._middleware.close()
            logging.info("action: shutdown_replica | result: success")
        except Exception as e:
            logging.error(f"action: shutdown_replica | result: fail | error: {e}")

        self._middleware.check_closed()

    def _handle_sigterm(self, sig, frame):
        """Maneja la señal SIGTERM para cerrar la réplica de forma segura."""
        logging.info("action: Received SIGTERM | shutting down gracefully.")
        self._shutdown()

    def process_replica_message(self, ch, method, properties, raw_message):
        """Procesa mensajes de la cola `Q_REPLICA`."""
        try:
            # ==================================================================
            # CAIDA LUEGO DE CONSUMIR MENSAJE Y ANTES DE DAR EL ACK
            # simulate_random_failure(self, log_with_location("CAIDA LUEGO DE CONSUMIR MENSAJE Y ANTES DE DAR EL ACK"), probability=REPLICAS_PROB_FAILURE)
            # ==================================================================
            msg = decode_msg(raw_message)
            # Determinar si la réplica necesita sincronización
            if msg.msg_id > 0 and not self.synchronized:

                # devuelvo el mensaje a la cola (si el msg_id es > 0 se trata de un push)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                # ==================================================================
                # CAIDA LUEGO DE DEVOLVER A LA COLA CON NACK
                simulate_random_failure(self, log_with_location("CAIDA LUEGO DE DEVOLVER A LA COLA CON NACK"), probability=REPLICAS_PROB_FAILURE)
                # ==================================================================

                ch.stop_consuming()
                logging.info(f"Replica {self.id}: Recibiendo primer mensaje con ID {msg.msg_id} de tipo {msg.type}. Iniciando sincronización.")
                return # ya me sincronicé y me vuelvo a consumir por la cola principal
            

            elif msg.type == MsgType.PUSH_DATA:
                # Procesar solo mensajes con un ID mayor al último procesado
                self._process_push_data(msg)

                # ==================================================================
                # CAIDA POST PROCESAR MENSAJE PUSH Y ANTES DE DAR EL ACK
                simulate_random_failure(self, log_with_location("CAIDA POST PROCESAR MENSAJE PUSH Y ANTES DE DAR EL ACK"), probability=REPLICAS_PROB_FAILURE)
                # ==================================================================
            
            elif msg.type == MsgType.FIN:
                self._process_fin_message(msg)

            # Confirmar la recepción del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # ==================================================================
            # CAIDA ACA POST PROCESAR MENSAJE Y DESPUES DE DAR EL ACK
            simulate_random_failure(self, log_with_location("CAIDA ACA POST PROCESAR MENSAJE Y DESPUES DE DAR EL ACK"), probability=REPLICAS_PROB_FAILURE)
            # ==================================================================

        except Exception as e:
            logging.error(f"action: process_replica_message | result: fail | error: {e.with_traceback()}")
    
    def recover_state(self):
        """
        Solicita el estado a las réplicas compañeras y se sincroniza.
        """
        logging.info(f"Replica {self.id}: Solicitando estado a réplicas compañeras.")

        # ==================================================================
        # CAIDA LUEGO DE ENTRAR A RECOVER_STATE Y ANTES DE ENVIAR SYNC_MSG
        simulate_random_failure(self, log_with_location("CAIDA LUEGO DE ENTRAR A RECOVER_STATE Y ANTES DE ENVIAR SYNC_MSG"), probability=REPLICAS_PROB_FAILURE)
        # ==================================================================

        # Crear una cola anónima y vincularla al exchange E_SYNC_STATE_REQUEST con la routing key basada en el ID de la réplica
        self.sync_anonymous_queue = self._middleware.declare_anonymous_queue(exchange_name=self.sync_exchange, routing_key=str(self.id))

        # Enviar Sync_state
        sync_msg = SimpleMessage(type=MsgType.SYNC_STATE_REQUEST, requester_id=self.id)
        # Lo envio al exchange sync donde escuchan los procesos externos
        self._middleware.send_to_queue(self.sync_request_listener_exchange, sync_msg.encode(), key="sync")

        # ==================================================================
        # CAIDA LUEGO DE ENVIAR SYNC_MSG Y ANTES DE ESPERAR RESPUESTA
        simulate_random_failure(self, log_with_location("CAIDA LUEGO DE ENVIAR SYNC_MSG Y ANTES DE ESPERAR RESPUESTA"), probability=REPLICAS_PROB_FAILURE)
        # ==================================================================

        responses = set()

        # Esperar respuesta
        def on_state_response(ch, method, properties, body):
            nonlocal responses
            msg = decode_msg(body)

            if not msg.node_id in responses:
                if isinstance(msg, SimpleMessage) and msg.type == MsgType.EMPTY_STATE:
                    logging.info(f"Replica {self.id}: Recibido estado vacío de réplica {msg.node_id}. ")
                    responses.add(msg.node_id)

                elif isinstance(msg, PushDataMessage):
                    last_msg_id = msg.data['last_msg_id']
                    logging.info(f"Replica {self.id}: Recibido estado completo de réplica {msg.node_id}. last_msg_id = {last_msg_id}")
                    if msg.data["last_msg_id"] > self.last_msg_id:
                        self._load_state(msg)
                    responses.add(msg.node_id)
                    # logging.info(f"Replica {self.id}: Estado recuperado de la réplica compañera.")
                
            ch.basic_ack(delivery_tag=method.delivery_tag)
            # Detener el consumo si ya se recibió una respuesta de cada réplica
            if len(responses) >= self.n_replicas-1:
                logging.info("recibi todas las respues de mis compañeras")
                ch.stop_consuming()
                # Eliminar la cola después de procesar el mensaje
                self._middleware.delete_queue(self.sync_anonymous_queue)

            # ==================================================================
            # CAIDA LUEGO DE HACER LOAD Y ANTES DE DAR ACK AL SYNC_MSG
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE HACER LOAD Y ANTES DE DAR ACK AL SYNC_MSG"), probability=REPLICAS_PROB_FAILURE)
            # ==================================================================

            # ==================================================================
            # CAIDA LUEGO DE HACER LOAD Y LUEGO DE DAR ACK AL SYNC_MSG
            simulate_random_failure(self, log_with_location("CAIDA LUEGO DE HACER LOAD Y LUEGO DE DAR ACK AL SYNC_MSG"), probability=REPLICAS_PROB_FAILURE)
            # ==================================================================

        self._middleware.receive_from_queue(self.sync_anonymous_queue, on_state_response, auto_ack=False)
        self.synchronized = True

    def _run_sync_listener(self):
        """
        Proceso dedicado a escuchar mensajes SYNC_STATE, utilizando su propio Middleware.
        """
        _sync_middleware = Middleware()

        def _process_sync_message(ch, method, properties, raw_message):
            """Procesa mensajes de tipo SYNC_STATE_REQUEST."""
            try:
                msg = decode_msg(raw_message)
                if msg.type == MsgType.CLOSE:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    ch.stop_consuming()
                    return
                elif msg.type == MsgType.SYNC_STATE_REQUEST and msg.requester_id != self.id:
                    logging.info(f"Replica {self.id}: Procesando mensaje de sincronización de réplica {msg.requester_id}.")

                    with self.lock:
                        answer = self._create_pull_answer() if self.synchronized else SimpleMessage(type=MsgType.EMPTY_STATE, node_id = self.id)

                    _sync_middleware.send_to_queue(self.sync_exchange, answer.encode(), str(msg.requester_id))
                    logging.info(f"envie estado a replica a {self.sync_exchange}, {msg.requester_id}")
                
                elif msg.type == MsgType.PULL_DATA:
                    logging.info(f"Replica {self.id}: Procesando mensaje de pull de master.")

                    with self.lock:
                        answer = self._create_pull_answer() if self.synchronized else SimpleMessage(type=MsgType.EMPTY_STATE, node_id = self.id)

                    _sync_middleware.send_to_queue(self.send_exchange, answer.encode())
                    logging.info(f"envie pull a master a {self.send_exchange}")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"Replica {self.id}: Error procesando mensaje en sync_state_: {e.with_traceback()}")

        try:
            _sync_middleware.declare_exchange(self.sync_exchange, type='fanout')
            _sync_middleware.declare_exchange(self.sync_request_listener_exchange)
            _sync_middleware.declare_queue(self.sync_request_listener_queue)
            # A ESTE EXCHANGE ENVIO LOS ESTADOS AL MASTER -> LE RESPONDO LOS PULL
            self.send_exchange = E_FROM_REPLICA_PULL_ANS + f'_{self.master_name}'
            _sync_middleware.declare_exchange(self.send_exchange)
            _sync_middleware.bind_queue(self.sync_request_listener_queue, self.sync_request_listener_exchange, "sync")
            _sync_middleware.bind_queue(self.sync_request_listener_queue, self.sync_request_listener_exchange, str(self.id))
            _sync_middleware.bind_queue(self.sync_request_listener_queue, self.sync_request_listener_exchange, "pull")
            _sync_middleware.receive_from_queue(self.sync_request_listener_queue, _process_sync_message, auto_ack=False)

        except Exception as e:
            logging.error(f"Replica {self.id}: Error en el listener SYNC_STATE: {e.with_traceback()}")
        finally:
            _sync_middleware.close()

def init_listener(id, container_name, port):
    listener = Listener(id, container_name, port)
    listener.run()