from collections import defaultdict
import logging
import threading
import signal
from messages.messages import MsgType, PushDataMessage, SimpleMessage, decode_msg
from middleware.middleware import Middleware
from replica import Replica
from utils.utils import NodeType, log_with_location, simulate_random_failure


class Q4JoinerReplica(Replica):

    def __init__(self, id: int, container_name: str, master_name: str, n_replicas: int):
        super().__init__(id, container_name, master_name, n_replicas)

        # Hilo separado para escuchar SYNC_STATE_REQUESTS
        self.sync_listener_thread = threading.Thread(
            target=_run_sync_listener,
            args=(
                self.id,
                self.sync_request_listener_exchange,
                self.sync_request_listener_queue,
                self.sync_exchange,
                self.shared_state,
                self.lock,
            ),
            daemon=True
        )
        self.sync_listener_thread.start()

    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Q4Joiner."""
        self.shared_state["negative_reviews_count_per_client"] = defaultdict(lambda: defaultdict(int))
        self.shared_state["games_per_client"] = defaultdict(dict)
        self.shared_state["negative_reviews_per_client"] = defaultdict(lambda: defaultdict(lambda: ([], False)))
        self.shared_state["fins_per_client"] = defaultdict(lambda: [False, False])
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.Q4_JOINER_REPLICA

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        with self.lock:
            response_data = PushDataMessage(
                data={
                    "last_msg_id": self.shared_state["last_msg_id"],
                    "negative_reviews_count_per_client": {
                        k: dict(v) for k, v in self.shared_state["negative_reviews_count_per_client"].items()
                    },
                    "games_per_client": dict(self.shared_state["games_per_client"]),
                    "negative_reviews_per_client": {
                        k: {app_id: (list(reviews), processed) for app_id, (reviews, processed) in v.items()}
                        for k, v in self.shared_state["negative_reviews_per_client"].items()
                    },
                    "fins_per_client": dict(self.shared_state["fins_per_client"]),
                },
                node_id=self.id,
            )
        return response_data

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        if msg.msg_id > self.shared_state["last_msg_id"] or msg.msg_id == 0:
            update_type = state.get("type")
            client_id = state.get("id")

            with self.lock:
                if update_type == "reviews":
                    self._update_negative_reviews(client_id, state.get("update", {}))
                elif update_type == "reviews_count":
                    self._update_negative_reviews_count(client_id, state.get("update", {}))
                elif update_type == "games":
                    self._update_games(client_id, state.get("update", {}))
                elif update_type == "fins":
                    self._update_fins(client_id, state.get("update", []))
                elif update_type == "delete":
                    self._delete_client_state(client_id)
                else:
                    logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

                self.shared_state["last_msg_id"] = msg.msg_id
                self.shared_state["sincronizado"] = True

    def _update_negative_reviews(self, client_id: int, updates: dict):
        """Actualiza las reseñas negativas de un cliente en la réplica."""
        client_reviews = self.shared_state["negative_reviews_per_client"].get(client_id, {})
        for app_id, value in updates.items():
            client_reviews[app_id] = value
        self.shared_state["negative_reviews_per_client"][client_id] = client_reviews

    def _update_negative_reviews_count(self, client_id: int, updates: dict):
        """Actualiza la cantidad de reseñas negativas de un cliente en la réplica."""
        client_reviews = self.shared_state["negative_reviews_count_per_client"].get(client_id, {})
        for app_id, count in updates.items():
            client_reviews[app_id] = count
        self.shared_state["negative_reviews_count_per_client"][client_id] = client_reviews

    def _update_games(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        client_games = self.shared_state["games_per_client"].get(client_id, {})
        for app_id, name in updates.items():
            client_games[app_id] = name
        self.shared_state["games_per_client"][client_id] = client_games

    def _update_fins(self, client_id: int, updates: list):
        """Actualiza los estados de FIN de un cliente en la réplica."""
        if len(updates) == 2:
            self.shared_state["fins_per_client"][client_id] = updates
        else:
            logging.warning(f"Replica: Formato inválido para actualización de fins: {updates}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.shared_state["negative_reviews_count_per_client"].pop(client_id, None)
        self.shared_state["games_per_client"].pop(client_id, None)
        self.shared_state["negative_reviews_per_client"].pop(client_id, None)
        self.shared_state["fins_per_client"].pop(client_id, None)
        logging.info(f"Replica: Estado borrado para client_id: {client_id}")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data
        with self.lock:
            if "negative_reviews_count_per_client" in state:
                for client_id, reviews in state["negative_reviews_count_per_client"].items():
                    self._update_negative_reviews_count(client_id, reviews)
            if "games_per_client" in state:
                for client_id, games in state["games_per_client"].items():
                    self._update_games(client_id, games)
            if "negative_reviews_per_client" in state:
                for client_id, reviews in state["negative_reviews_per_client"].items():
                    self._update_negative_reviews(client_id, reviews)
            if "fins_per_client" in state:
                for client_id, fins in state["fins_per_client"].items():
                    self._update_fins(client_id, fins)
            if "last_msg_id" in state:
                self.shared_state["last_msg_id"] = state["last_msg_id"]

            self.shared_state["sincronizado"] = True
            logging.info(f"Replica: Estado completo cargado con last_msg_id {self.shared_state['last_msg_id']}.")


def _run_sync_listener(replica_id, listening_exchange, listening_queue, sync_exchange, shared_state, lock):
    """
    Hilo dedicado a escuchar mensajes SYNC_STATE, utilizando su propio Middleware.
    """
    sync_middleware = Middleware()
    sigterm_received = False

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
                                "negative_reviews_count_per_client": {
                                    k: dict(v) for k, v in shared_state["negative_reviews_count_per_client"].items()
                                },
                                "games_per_client": dict(shared_state["games_per_client"]),
                                "negative_reviews_per_client": {
                                    k: {app_id: (list(reviews), processed) for app_id, (reviews, processed) in v.items()}
                                    for k, v in shared_state["negative_reviews_per_client"].items()
                                },
                                "fins_per_client": dict(shared_state["fins_per_client"]),
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
        sync_middleware.declare_exchange(listening_exchange, type="fanout")
        sync_middleware.declare_queue(listening_queue)
        sync_middleware.bind_queue(listening_queue, listening_exchange)

        sync_middleware.receive_from_queue(listening_queue, _process_sync_message, auto_ack=False)
    except Exception as e:
        if not sigterm_received:
            logging.error(f"Replica {replica_id}: Error en el listener SYNC_STATE: {e}")
        else:
            logging.info(f"Replica {replica_id}: Listener SYNC_STATE cerrado de forma intencionada.")
