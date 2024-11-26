from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class Q3JoinerReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Q3Joiner."""
        self.games_per_client = defaultdict(dict)  # Juegos por cliente (client_id -> {app_id: name})
        self.review_counts_per_client = defaultdict(lambda: defaultdict(int))  # Reseñas por cliente (client_id -> app_id -> count)
        self.fins_per_client = defaultdict(lambda: [False, False])  # Fins por cliente (client_id -> [fin_games, fin_reviews])

        self.state = {
            "games_per_client": self.games_per_client,
            "review_counts_per_client": self.review_counts_per_client,
            "fins_per_client": self.fins_per_client,
        }
        logging.info("Replica: Almacenamiento inicializado.")

    def _process_pull_data(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        if not self.state:
            logging.warning("Replica: Estado no inicializado, enviando estado vacío.")
            self.state = {
                "games_per_client": {},
                "review_counts_per_client": {},
                "fins_per_client": {}
            }
        response_data = PushDataMessage( data={
            "games_per_client": dict(self.games_per_client),
            "review_counts_per_client": {k: dict(v) for k, v in self.review_counts_per_client.items()},
            "fins_per_client": dict(self.fins_per_client),
        })
        self._middleware.send_to_queue(self.send_queue, response_data.encode())
        logging.info("Replica: Estado completo enviado en respuesta a PullDataMessage.")


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

    def _update_games(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        client_games = self.games_per_client[client_id]
        for app_id, name in updates.items():
            client_games[app_id] = name
        # logging.info(f"Replica: Juegos actualizados para client_id: {client_id} | updates: {updates}")

    def _update_reviews(self, client_id: int, updates: dict):
        """Actualiza las reseñas de un cliente en la réplica."""
        client_reviews = self.review_counts_per_client[client_id]
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
        self.review_counts_per_client.pop(client_id, None)
        self.fins_per_client.pop(client_id, None)
        # logging.info(f"Replica: Estado borrado para client_id: {client_id}")