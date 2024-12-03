from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica
from utils.utils import NodeType

class Q5JoinerReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Q3Joiner."""
        self.games_per_client = defaultdict(lambda: {})  # Almacena juegos por `app_id`, para cada cliente
        self.negative_review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Contador de reseñas negativas por `app_id`
        self.fins_per_client = defaultdict(lambda: [False, False])  # Fins por cliente (client_id -> [fin_games, fin_reviews])
        self.last_msg_id = 0

        self.state = {
            "last_msg_id": self.last_msg_id,
            "games_per_client": self.games_per_client,
            "negative_review_counts_per_client": self.negative_review_counts_per_client,
            "fins_per_client": self.fins_per_client,
        }
        logging.info("Replica: Almacenamiento inicializado.")

    def get_type(self):
        return NodeType.Q5_JOINER_REPLICA

    def _create_pull_answer(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        if not self.state:
            logging.warning("Replica: Estado no inicializado, enviando estado vacío.")
            self.state = {
                "last_msg_id": 0,
                "games_per_client": {},
                "negative_review_counts_per_client": {},
                "fins_per_client": {}
            }
        response_data = PushDataMessage( data={
            'last_msg_id': self.state['last_msg_id'],
            "games_per_client": dict(self.games_per_client),
            "negative_review_counts_per_client": {k: dict(v) for k, v in self.negative_review_counts_per_client.items()},
            "fins_per_client": dict(self.fins_per_client),
        }, node_id=self.id)
        return response_data


    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        # Identificar el tipo de actualización
        update_type = state.get("type")
        client_id = state.get("id")

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

        # Actualizar last_msg_id después de procesar un mensaje válido
        self.state["last_msg_id"] = msg.msg_id
        # logging.info(f"Replica: Mensaje PUSH procesado con ID {msg.msg_id}. Estado actualizado.")

    def _update_games(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        client_games = self.games_per_client[client_id]
        for app_id, name in updates.items():
            client_games[app_id] = name
        # logging.info(f"Replica: Juegos actualizados para client_id: {client_id} | updates: {updates}")

    def _update_reviews(self, client_id: int, updates: dict):
        """Actualiza las reseñas de un cliente en la réplica."""
        client_reviews = self.negative_review_counts_per_client[client_id]
        for app_id, count in updates.items():
            client_reviews[app_id] = count
        # logging.info(f"Replica: Reseñas actualizadas para client_id: {client_id} | updates: {updates}")

    def _update_fins(self, client_id: int, updates: list):
        """Actualiza los estados de FIN de un cliente en la réplica."""
        if len(updates) != 2:
            logging.warning(f"Replica: Formato inválido para actualización de fins: {updates}")
            return
        self.fins_per_client[client_id] = updates
        # # logging.info(f"Replica: Estado de FIN actualizado para client_id: {client_id} | fins: {updates}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.games_per_client.pop(client_id, None)
        self.negative_review_counts_per_client.pop(client_id, None)
        self.fins_per_client.pop(client_id, None)
        # logging.info(f"Replica: Estado borrado para client_id: {client_id}")

    def _load_state(self, msg: PushDataMessage):
        """Carga el estado completo recibido en la réplica."""
        state = msg.data

        # Actualizar juegos por cliente
        if "games_per_client" in state:
            for client_id, games in state["games_per_client"].items():
                self.games_per_client[client_id] = games
            logging.info(f"Replica: Juegos actualizados desde estado recibido.")

        # Actualizar reseñas por cliente
        if "negative_review_counts_per_client" in state:
            for client_id, reviews in state["negative_review_counts_per_client"].items():
                if client_id not in self.negative_review_counts_per_client:
                    self.negative_review_counts_per_client[client_id] = defaultdict(int)
                self.negative_review_counts_per_client[client_id].update(reviews)
            logging.info(f"Replica: Reseñas actualizadas desde estado recibido.")

        # Actualizar fins por cliente
        if "fins_per_client" in state:
            for client_id, fins in state["fins_per_client"].items():
                self.fins_per_client[client_id] = fins
            logging.info(f"Replica: Estados FIN actualizados desde estado recibido.")

        # Actualizar el último mensaje procesado (last_msg_id)
        if "last_msg_id" in state:
            self.last_msg_id = state["last_msg_id"]

        logging.info(f"Replica: Estado completo cargado. Campos cargados: {list(state.keys())}")
