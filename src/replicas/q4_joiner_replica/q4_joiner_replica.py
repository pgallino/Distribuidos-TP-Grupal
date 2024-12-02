from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class Q4JoinerReplica(Replica):
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Q4Joiner."""
        self.negative_reviews_count_per_client = defaultdict(lambda: defaultdict(int))  # Contar reseñas negativas por cliente y juego
        self.games_per_client = defaultdict(dict)  # Detalles de juegos de acción/shooter (client_id -> {app_id: name})
        self.negative_reviews_per_client = defaultdict(lambda: defaultdict(lambda: ([], False)))  # Reviews negativas y estado (client_id -> app_id -> (reviews, processed))
        self.fins_per_client = defaultdict(lambda: [False, False])  # Fines por cliente (client_id -> [fin_games, fin_reviews])
        self.last_msg_id = 0

        self.state = {
            "last_msg_id": self.last_msg_id,
            "negative_reviews_count_per_client": self.negative_reviews_count_per_client,
            "games_per_client": self.games_per_client,
            "negative_reviews_per_client": self.negative_reviews_per_client,
            "fins_per_client": self.fins_per_client,
        }
        logging.info("Replica: Almacenamiento inicializado.")

    def _create_pull_answer(self):
        if not self.state:
            logging.warning("Replica: Estado no inicializado, enviando estado vacío.")
            self.state = {
                "negative_reviews_count_per_client": {},
                "games_per_client": {},
                "negative_reviews_per_client": {},
                "fins_per_client": {}
            }
        response_data = PushDataMessage( data={
            "negative_reviews_count_per_client": {
                k: dict(v) for k, v in self.negative_reviews_count_per_client.items()
            },
            "games_per_client": dict(self.games_per_client),
            "negative_reviews_per_client": {
                k: {app_id: (list(reviews), processed) for app_id, (reviews, processed) in v.items()}
                for k, v in self.negative_reviews_per_client.items()
            },
            "fins_per_client": dict(self.fins_per_client),
        }, last_msg_id=self.last_msg_id, msg_id=self.id)

        return response_data

    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        # Identificar el tipo de actualización
        update_type = state.get("type")
        client_id = state.get("id")

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

        # Actualizar last_msg_id después de procesar un mensaje válido
        self.state["last_msg_id"] = msg.msg_id
        # logging.info(f"Replica: Mensaje PUSH procesado con ID {msg.msg_id}. Estado actualizado.")

    def _update_negative_reviews(self, client_id: int, updates: dict):
        """Actualiza las reseñas negativas de un cliente en la réplica."""
        client_reviews = self.negative_reviews_per_client[client_id]
        for app_id, tuple in updates.items():
            client_reviews[app_id] = tuple
        # logging.info(f"Replica: Reseñas negativas actualizadas para client_id: {client_id} | updates: {updates}")

    def _update_negative_reviews_count(self, client_id: int, updates: dict):
        """Actualiza las reseñas negativas de un cliente en la réplica."""
        client_reviews = self.negative_reviews_count_per_client[client_id]
        for app_id, count in updates.items():
            client_reviews[app_id] = count
        # logging.info(f"Replica: Reseñas negativas actualizadas para client_id: {client_id} | updates: {updates}")

    def _update_games(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        client_games = self.games_per_client[client_id]
        for app_id, name in updates.items():
            client_games[app_id] = name
        # logging.info(f"Replica: Juegos actualizados para client_id: {client_id} | updates: {updates}")

    def _update_fins(self, client_id: int, updates: list):
        """Actualiza los estados de FIN de un cliente en la réplica."""
        if len(updates) != 2:
            logging.warning(f"Replica: Formato inválido para actualización de fins: {updates}")
            return
        self.fins_per_client[client_id] = updates
        # logging.info(f"Replica: Estado de FIN actualizado para client_id: {client_id} | fins: {updates}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.negative_reviews_count_per_client.pop(client_id, None)
        self.games_per_client.pop(client_id, None)
        self.negative_reviews_per_client.pop(client_id, None)
        self.fins_per_client.pop(client_id, None)
        logging.info(f"Replica: Estado borrado para client_id: {client_id}")
