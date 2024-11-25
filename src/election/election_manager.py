import logging
from multiprocessing import Condition, Process, Value
from election.election_listener import ElectionListener
from election.election_logic import initiate_election

def init_election_listener(id, ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart):
    e_listener = ElectionListener(id, ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart)
    e_listener.run()

class ElectionManager:
    def __init__(self, id, ids, container_name, on_leader_selected, container_to_restart):
        """
        Inicializa el ElectionManager.

        Args:
            id (int): ID de la réplica actual.
            ids (list): Lista de IDs de nodos en el sistema.
            container_name (str): Nombre del container que ejecuta esta logica
            container_to_restart (str): Nombre del container master
            on_leader_selected (function): Función genérica a ejecutar al seleccionarse como líder.
            *args: Argumentos posicionales para `on_leader_selected`.
            **kwargs: Argumentos clave para `on_leader_selected`.
        """
        self.node_id = id
        self.node_ids = ids
        self.container_name = container_name
        self.election_in_progress = Value('i', False)  # 0 = No, 1 = Sí
        self.condition = Condition()
        self.waiting_ok_election = Value('i', False)  # 0 = No, 1 = Sí
        self.condition_ok = Condition()
        self.on_leader_selected = on_leader_selected
        self.container_to_restart = container_to_restart
        self.listener_process = None

        self.start_listener()


    def start_listener(self):
        """Levanta el proceso del ElectionListener."""
        logging.info(f"ElectionManager {self.node_id}: Iniciando listener de elecciones.")
        if self.listener_process is None or not self.listener_process.is_alive():
            self.listener_process = Process(
                target=init_election_listener,
                args=(
                    self.node_id,
                    self.node_ids,
                    self.container_name,
                    self.election_in_progress,
                    self.condition,
                    self.waiting_ok_election,
                    self.condition_ok,
                    self.on_leader_selected,
                    self.container_to_restart
                ),
            )
            self.listener_process.start()

    def manage_leadership(self):
        """Maneja un error de conexión detectando si es necesario iniciar una elección."""
        logging.info(f"ElectionManager {self.node_id}: manage_leadership")
        with self.condition:
            if self.election_in_progress.value:
                logging.info("Elección ya en proceso. Esperando a que termine...")
                self.condition.wait()  # Espera a que termine la elección
                return
            self.election_in_progress.value = True

        initiate_election(
            self.node_id,
            self.node_ids,
            self.container_name,
            self.election_in_progress,
            self.condition,
            self.waiting_ok_election,
            self.condition_ok,
            self.on_leader_selected,
            self.container_to_restart
        )

    def cleanup(self):
        """Limpia todos los recursos manejados por el ElectionManager."""
        logging.info(f"ElectionManager {self.node_id}: Iniciando limpieza de recursos.")

        # Detener el listener si está activo
        if self.listener_process and self.listener_process.is_alive():
            logging.info(f"ElectionManager {self.node_id}: Deteniendo listener de elecciones.")
            self.listener_process.terminate()
            self.listener_process.join()
        
        # Resetear el estado de las variables compartidas
        with self.condition:
            self.election_in_progress.value = False
            self.condition.notify_all()  # Notificar a los hilos en espera

        with self.condition_ok:
            self.waiting_ok_election.value = False
            self.condition_ok.notify_all()  # Notificar a los hilos en espera

        logging.info(f"ElectionManager {self.node_id}: Recursos limpiados exitosamente.")

