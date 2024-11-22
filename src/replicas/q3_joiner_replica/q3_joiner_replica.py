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
        response_data = PushDataMessage(data={
            "games_per_client": dict(self.games_per_client),
            "review_counts_per_client": {k: dict(v) for k, v in self.review_counts_per_client.items()},
            "fins_per_client": dict(self.fins_per_client),
        })
        self._middleware.send_to_queue(self.send_queue, response_data.encode())
        logging.info("Replica: Estado completo enviado en respuesta a PullDataMessage.")


    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        state = msg.data

        # Actualizar o borrar un cliente
        if "delete" in state:
            client_id = state["delete"]
            # Borrar todas las referencias al cliente
            self._delete_client_state(client_id)
            return

        client_id = state.get("client_id")
        if client_id is None:
            logging.warning("Replica: Mensaje recibido sin 'client_id', no se puede procesar.")
            return

        # Actualizar juegos si están presentes
        if "games" in state:
            self.games_per_client[client_id] = state["games"]
            # logging.info(f"Replica: Juegos actualizados para client_id: {client_id} | games: {state['games']}")

        # Actualizar reseñas si están presentes
        if "reviews" in state:
            if client_id not in self.review_counts_per_client:
                self.review_counts_per_client[client_id] = defaultdict(int)
            self.review_counts_per_client[client_id].update(state["reviews"])
            # logging.info(f"Replica: Reseñas actualizadas para client_id: {client_id} | reviews: {state['reviews']}")

        # Actualizar fins si están presentes
        if "fins" in state:
            self.fins_per_client[client_id] = state["fins"]
            # logging.info(f"Replica: Estado de FIN actualizado para client_id: {client_id} | fins: {state['fins']}")

        # logging.info(f"action: Update state | client_id: {client_id} | updated fields: {list(state.keys())}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.games_per_client.pop(client_id, None)
        self.review_counts_per_client.pop(client_id, None)
        self.fins_per_client.pop(client_id, None)
        logging.info(f"Replica: Estado borrado para client_id: {client_id}")