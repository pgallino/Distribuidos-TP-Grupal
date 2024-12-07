from collections import defaultdict
import logging
from messages.messages import PushDataMessage
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

    def get_type(self):
        return NodeType.Q5_JOINER_REPLICA

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
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