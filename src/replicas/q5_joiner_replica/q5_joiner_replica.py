from collections import defaultdict
import logging
import threading
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from replica import Replica
from utils.utils import NodeType

class Q5JoinerReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Q5Joiner."""
        # Inicialización de almacenamiento
        self.games_per_client = defaultdict(dict)
        self.negative_review_counts_per_client = defaultdict(lambda: defaultdict(int))
        self.fins_per_client = defaultdict(lambda: [False, False])

        # Variables de estado compartido
        self.state_vars = (
            self.synchronized,
            self.last_msg_id,
            self.games_per_client,
            self.negative_review_counts_per_client,
            self.fins_per_client
        )
        logging.info("Replica: Almacenamiento inicializado.")

        # Hilo para manejar solicitudes de sincronización
        self.sync_listener_thread = threading.Thread(
            target=_run_sync_listener,
            args=(
                self.id,
                self.sync_request_listener_exchange,
                self.sync_request_listener_queue,
                self.sync_exchange,
                self.state_vars,
                self.lock,
            ),
            daemon=True
        )
        self.sync_listener_thread.start()

    def get_type(self):
        return NodeType.Q5_JOINER_REPLICA

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        with self.lock:
            response_data = PushDataMessage(
                data={
                    "last_msg_id": self.last_msg_id,
                    "games_per_client": dict(self.games_per_client),
                    "negative_review_counts_per_client": {
                        k: dict(v) for k, v in self.negative_review_counts_per_client.items()
                    },
                    "fins_per_client": dict(self.fins_per_client),
                },
                node_id=self.id,
            )
        return response_data

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        if msg.msg_id > self.last_msg_id or msg.msg_id == 0:
            update_type = state.get("type")
            client_id = state.get("id")

            with self.lock:
                if update_type == "games":
                    self._update_games(client_id, state.get("update", {}))
                elif update_type == "reviews":
                    self._update_reviews(client_id, state.get("update", {}))
                elif update_type == "fins":
                    self._update_fins(client_id, state.get("update", []))
                elif update_type == "delete":
                    self._delete_client_state(client_id)
                else:
                    logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

                self.last_msg_id = msg.msg_id
                self.synchronized = True

    def _update_games(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        for app_id, name in updates.items():
            self.games_per_client[client_id][app_id] = name

    def _update_reviews(self, client_id: int, updates: dict):
        """Actualiza las reseñas negativas de un cliente en la réplica."""
        for app_id, count in updates.items():
            self.negative_review_counts_per_client[client_id][app_id] = count

    def _update_fins(self, client_id: int, updates: list):
        """Actualiza los estados de FIN de un cliente en la réplica."""
        if len(updates) == 2:
            self.fins_per_client[client_id] = updates
        else:
            logging.warning(f"Replica: Formato inválido para actualización de fins: {updates}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.games_per_client.pop(client_id, None)
        self.negative_review_counts_per_client.pop(client_id, None)
        self.fins_per_client.pop(client_id, None)
        logging.info(f"Replica: Estado eliminado para client_id: {client_id}")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data
        with self.lock:
            if "games_per_client" in state:
                for client_id, games in state["games_per_client"].items():
                    self._update_games(client_id, games)
            if "negative_review_counts_per_client" in state:
                for client_id, reviews in state["negative_review_counts_per_client"].items():
                    self._update_reviews(client_id, reviews)
            if "fins_per_client" in state:
                for client_id, fins in state["fins_per_client"].items():
                    self._update_fins(client_id, fins)
            if "last_msg_id" in state:
                self.last_msg_id = state["last_msg_id"]

        self.synchronized = True
        logging.info(f"recibi este estado {self.last_msg_id}")


def _run_sync_listener(replica_id, listening_exchange, listening_queue, sync_exchange, state_vars, lock):
    """
    Hilo dedicado a escuchar mensajes SYNC_STATE.
    """
    sync_middleware = Middleware()
    synchronized, last_msg_id, games_per_client, negative_review_counts_per_client, fins_per_client = state_vars

    def _process_sync_message(ch, method, properties, raw_message):
        """Procesa mensajes de tipo SYNC_STATE_REQUEST."""
        try:
            msg = decode_msg(raw_message)
            if msg.type == MsgType.CLOSE:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()
                return
            if msg.type == MsgType.SYNC_STATE_REQUEST and msg.requester_id != replica_id:
                logging.info(f"Replica {replica_id}: Procesando mensaje de sincronización de réplica {msg.requester_id}.")

                with lock:
                    if synchronized:
                        answer = PushDataMessage(
                            data={
                                "last_msg_id": last_msg_id,
                                "games_per_client": dict(games_per_client),
                                "negative_review_counts_per_client": {
                                    k: dict(v) for k, v in negative_review_counts_per_client.items()
                                },
                                "fins_per_client": dict(fins_per_client),
                            },
                            node_id=replica_id,
                        )
                    else:
                        answer = SimpleMessage(type=MsgType.EMPTY_STATE, node_id=replica_id)

                sync_middleware.send_to_queue(sync_exchange, answer.encode(), str(msg.requester_id))
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Replica {replica_id}: Error procesando mensaje SYNC_STATE: {e}")

    try:
        sync_middleware.declare_exchange(sync_exchange, type='fanout')
        sync_middleware.declare_exchange(listening_exchange)
        sync_middleware.declare_queue(listening_queue)
        sync_middleware.bind_queue(listening_queue, listening_exchange, "sync")
        sync_middleware.bind_queue(listening_queue, listening_exchange, str(replica_id))
        sync_middleware.receive_from_queue(listening_queue, _process_sync_message, auto_ack=False)
        logging.info("SALI CON CLOSE")
    except Exception as e:
        logging.error(f"Replica {replica_id}: Error en el listener SYNC_STATE: {e}")
    finally:
        sync_middleware.close()
