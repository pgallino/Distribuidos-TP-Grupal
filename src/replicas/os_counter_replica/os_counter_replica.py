from multiprocessing import Manager, Process
import logging
import signal
import sys
from collections import defaultdict
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from replica import Replica
from utils.utils import NodeType

class OsCounterReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        manager = Manager()
        super().__init__(id, container_name, master_name, n_replicas, manager)

        # Estado específico inicializado con Manager para compartir entre procesos
        self.shared_state["os_count"] = manager.dict()

        # Proceso separado para escuchar SYNC_STATE_REQUESTS
        self.sync_listener_process = Process(
            target=_run_sync_listener,
            args=(self.id, self.sync_request_listener_exchange, self.sync_request_listener_queue, self.sync_exchange, self.shared_state, self.lock)
        )
        self.sync_listener_process.start()

    def get_type(self):
        return NodeType.OS_COUNTER_REPLICA

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""

        if msg.msg_id > self.shared_state["last_msg_id"] or msg.msg_id == 0:

            state = msg.data
            # Identificar el tipo de actualización
            update_type = state.get("type")
            client_id = state.get("id")

            with self.lock:
                if update_type == "os_count":
                    updated_counters = state.get("update")
                    if updated_counters:
                        self.shared_state["os_count"][client_id] = updated_counters

                elif update_type == "delete":
                    self._delete_client_state(client_id)

                else:
                    logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

                # Actualizar last_msg_id después de procesar un mensaje válido
                self.shared_state["last_msg_id"] = msg.msg_id
                self.shared_state["sincronizado"] = True

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        with self.lock:
            response_data = PushDataMessage(data={
                "last_msg_id": self.shared_state["last_msg_id"],
                "os_count": dict(self.shared_state["os_count"])  # Convierte defaultdict a dict estándar
            }, node_id=self.id)
        return response_data

    def _delete_client_state(self, client_id):
        """Elimina el estado de un cliente específico sin usar `lock`."""
        if client_id in self.shared_state["os_count"]:
            del self.shared_state["os_count"][client_id]
            logging.info(f"Replica: Estado eliminado para cliente {client_id}.")
        else:
            logging.warning(f"Intento de eliminar estado inexistente para cliente {client_id}.")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        with self.lock:
            # Actualizar contadores de sistemas operativos por cliente
            if "os_count" in state:
                for client_id, counters in state["os_count"].items():
                    self.shared_state["os_count"][client_id] = tuple(counters)
                logging.info(f"Replica: Contadores de sistemas operativos actualizados desde estado recibido.")

            # Actualizar el último mensaje procesado
            if "last_msg_id" in state:
                self.shared_state["last_msg_id"] = state.get("last_msg_id")

            self.shared_state["sincronizado"] = True

def _run_sync_listener(replica_id, listening_exchange, listening_queue, sync_exchange, shared_state, lock):
    """
    Proceso dedicado a escuchar mensajes SYNC_STATE, utilizando su propio Middleware.
    """
    sync_middleware = Middleware()

    def handle_sigterm(signal_received, frame):
        """Manejador de señal SIGTERM para salir limpiamente."""
        logging.info(f"Replica {replica_id}: Señal SIGTERM recibida. Cerrando listener SYNC_STATE.")
        sync_middleware.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    def _process_sync_message(ch, method, properties, raw_message):
        """Procesa mensajes de tipo SYNC_STATE_REQUEST."""
        try:
            msg = decode_msg(raw_message)
            if msg.type == MsgType.SYNC_STATE_REQUEST and msg.requester_id != replica_id:
                logging.info(f"Replica {replica_id}: Procesando mensaje de sincronización de réplica {msg.requester_id}.")

                with lock:
                    if shared_state["sincronizado"]:
                        answer = PushDataMessage(
                            data={
                                "last_msg_id": shared_state["last_msg_id"],
                                "os_count": dict(shared_state["os_count"])
                            },
                            node_id=replica_id
                        )
                    else:
                        answer = SimpleMessage(type=MsgType.EMPTY_STATE, node_id=replica_id)

                logging.info(f"Replica {replica_id}: Le prepare este mensaje a {msg.requester_id}: {answer}.")
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
        logging.error(f"Replica {replica_id}: Error en el listener SYNC_STATE: {e}")
    finally:
        sync_middleware.close()
