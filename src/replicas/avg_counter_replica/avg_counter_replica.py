from multiprocessing import Manager, Process
import logging
import signal
import sys
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from replica import Replica
from utils.utils import NodeType

class AvgCounterReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

        # Proceso separado para escuchar SYNC_STATE_REQUESTS
        self.sync_listener_process = Process(
            target=_run_sync_listener,
            args=(self.id, self.sync_request_listener_exchange, self.sync_request_listener_queue, self.sync_exchange, self.shared_state, self.lock)
        )
        self.sync_listener_process.start()

    def _initialize_storage(self):
        self.shared_state["avg_count"] = self.manager.dict()

    def get_type(self):
        return NodeType.AVG_COUNTER_REPLICA

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""

        # Procesar solo mensajes con un ID mayor al último procesado
        if msg.msg_id > self.shared_state["last_msg_id"] or msg.msg_id == 0:

            state = msg.data

            # Identificar el tipo de actualización
            update_type = state.get("type")
            client_id = state.get("id")

            # logging.info("quiero tomar lock en push")
            with self.lock:
                # logging.info("tome lock en push")
                if update_type == "avg_count":
                    heap_data = state.get("update")
                    if heap_data:
                        # Actualizar el heap del cliente
                        self.shared_state["avg_count"][client_id] = heap_data

                elif update_type == "delete":
                    self._delete_client_state(client_id)

                # Actualizar last_msg_id después de procesar un mensaje válido
                self.shared_state["last_msg_id"] = msg.msg_id
                self.shared_state["sincronizado"] = True

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        # logging.info("quiero tomar lock en pull")
        with self.lock:
            # logging.info("tome lock en pull")
            response_data = PushDataMessage(data={
                "last_msg_id": self.shared_state["last_msg_id"],
                "avg_count": dict(self.shared_state["avg_count"])
            }, node_id=self.id)
        return response_data

    def _delete_client_state(self, client_id):
        """Elimina el estado de un cliente específico."""
        if client_id in self.shared_state["avg_count"]:
            del self.shared_state["avg_count"][client_id]
            logging.info(f"Replica: Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # logging.info("quiero tomar lock en load")
        with self.lock:
            # logging.info("tome lock en load")
            # Actualizar heaps por cliente
            if "avg_count" in state:
                for client_id, heap_data in state["avg_count"].items():
                    self.shared_state["avg_count"][client_id] = [tuple(item) for item in heap_data]
                logging.info(f"Replica: Heaps de promedio actualizados desde estado recibido.")

            # Actualizar el último mensaje procesado
            if "last_msg_id" in state:
                self.shared_state["last_msg_id"] = state["last_msg_id"]
            
            self.shared_state['sincronizado'] = True

def _run_sync_listener(replica_id, listening_exchange, listening_queue, sync_exchange, shared_state, lock):
    """
    Proceso dedicado a escuchar mensajes SYNC_STATE, utilizando su propio Middleware.
    """
    sync_middleware = Middleware()
    sigterm_received = False

    def handle_sigterm(signal_received, frame):
        nonlocal sync_middleware, sigterm_received
        """Manejador de señal SIGTERM para salir limpiamente."""
        logging.info(f"Replica {replica_id}: Señal SIGTERM recibida. Cerrando listener SYNC_STATE.")
        sigterm_received = True  # Indica que el cierre es intencionado
        sync_middleware.close()

    signal.signal(signal.SIGTERM, handle_sigterm)

    def _process_sync_message(ch, method, properties, raw_message):
        """Procesa mensajes de tipo SYNC_STATE_REQUEST."""
        try:
            msg = decode_msg(raw_message)
            if msg.type == MsgType.SYNC_STATE_REQUEST and msg.requester_id != replica_id:
                logging.info(f"Replica {replica_id}: Procesando mensaje de sincronización de réplica {msg.requester_id}.")

                # logging.info("quiero tomar lock en proceso externo")
                with lock:
                    # logging.info("tome lock en proceso externo")
                    if shared_state["sincronizado"]:
                        answer = PushDataMessage(
                            data={
                                "last_msg_id": shared_state["last_msg_id"],
                                "avg_count": dict(shared_state["avg_count"])
                            },
                            node_id=replica_id
                        )
                    else:
                        answer = SimpleMessage(type=MsgType.EMPTY_STATE, node_id=replica_id)

                logging.info(f"Replica {replica_id}: Le prepare este mensaje {answer}.")
                # Publicar el estado en el exchange con la routing key del solicitante
                sync_middleware.send_to_queue(
                    sync_exchange,
                    answer.encode(),
                    str(msg.requester_id)
                )
                logging.info(f"Replica {replica_id}: Envie el state a {sync_exchange} con key {str(msg.requester_id)}.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Replica {replica_id}: Error procesando mensaje SYNC_STATE: {e}")

    try:
        # Configurar su propio Middleware
        sync_middleware.declare_exchange(sync_exchange, type='fanout')
        sync_middleware.declare_exchange(listening_exchange, type="fanout")
        sync_middleware.declare_queue(listening_queue)
        sync_middleware.bind_queue(listening_queue, listening_exchange)

        sync_middleware.receive_from_queue(
            listening_queue,
            _process_sync_message,
            auto_ack=False
        )
    except Exception as e:
        if not sigterm_received:
            logging.error(f"Replica {replica_id}: Error en el listener SYNC_STATE: {e}")
        else:
            logging.info(f"Replica {replica_id}: Listener SYNC_STATE cerrado de forma intencionada.")
