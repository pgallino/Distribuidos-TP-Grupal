from collections import defaultdict
import logging
from messages.messages import PushDataMessage
from replica import Replica

class PropagatorReplica(Replica):
        
    def _initialize_storage(self):
        """Inicializa las estructuras de almacenamiento específicas para Propagator."""
        self.nodes_fins_state = {}
        logging.info("Replica: Almacenamiento inicializado.")

    def _process_pull_data(self):
        """Procesa un mensaje de solicitud de pull de datos."""
        response_data = PushDataMessage( data={
            "node_fin_state": self.nodes_fins_state
        })
        self._middleware.send_to_queue(self.send_queue, response_data.encode())
        logging.info("Replica: Estado completo enviado en respuesta a PullDataMessage.")


    def _process_push_data(self, msg: PushDataMessage):
        """Procesa los datos de un mensaje `PushDataMessage`."""
        update = msg.data

        # Identificar el tipo de actualización
        update_type = update.get("type")
        client_id = update.get("id")

        if update_type == "node_fin_state":
            self._node_fin_state(client_id, update.get("update", {}))
        if update_type == "delete":
            self._delete_client_state(client_id, update.get("update", {}))
        # alguno para crear client_state?
        else:
            logging.warning(f"Replica: Tipo de actualización desconocido '{update_type}' para client_id: {client_id}")

    def _node_fin_state(self, client_id: int, updates: dict):
        """Actualiza los juegos de un cliente en la réplica."""
        node_type, node_instance, value = updates['node_type'], updates['node_instance'], updates['value']
        if client_id not in self.nodes_fins_state:
            self._add_new_client_state(client_id)
        
        client_games = self.games_per_client[client_id]
        for app_id, name in updates.items():
            client_games[app_id] = name
        # logging.info(f"Replica: Juegos actualizados para client_id: {client_id} | updates: {updates}")

    def _delete_client_state(self, client_id: int):
        """Elimina todas las referencias al cliente en el estado."""
        self.nodes_fins_state.pop(client_id, None)
        # logging.info(f"Replica: Estado borrado para client_id: {client_id}")

    def _add_new_client_state(self, client_id: int):
        """
        Configura el estado inicial de los nodos.
        """
        initial_nodes_states = {}
        for name, instances in self.nodes_instances.items():
            initial_nodes_states[name] = {}
            
            for instance in range(1, instances+1):
                initial_nodes_states[name][instance] = False

            initial_nodes_states[name]['fins_propagated'] = 0
        
        logging.info(f'Se crea el diccionario {initial_nodes_states} para el cliente {client_id}')
        self.nodes_fins_state[client_id] = initial_nodes_states