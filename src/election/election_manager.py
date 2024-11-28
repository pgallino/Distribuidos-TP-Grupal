import logging
from multiprocessing import Condition, Process, Value
from election.election_listener import ElectionListener
from election.election_logic import initiate_election

def init_election_listener(id, ids, ip_prefix, port, election_in_progress, condition, waiting_ok, ok_condition, leader_id):
    e_listener = ElectionListener(id, ids, ip_prefix, port, election_in_progress, condition, waiting_ok, ok_condition, leader_id)
    e_listener.run()

class ElectionManager:
    def __init__(self, id, ids, ip_prefix, port):
        """
        Inicializa el ElectionManager.

        Args:
            id (int): ID de la réplica actual.
            ids (list): Lista de IDs de nodos en el sistema.
            ip_prefix (str): prefijo de la dirrección ip
            port (int): puerto a utilizar
        """
        self.node_id = id
        self.node_ids = ids
        self.ip_prefix = ip_prefix
        self.port = port
        self.election_in_progress = Value('i', False)  # 0 = No, 1 = Sí
        self.leader_id = Value('i', -1)  # Inicializado con -1 (ningún líder)
        self.condition = Condition()
        self.waiting_ok = Value('i', False)  # 0 = No, 1 = Sí
        self.ok_condition = Condition()
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
                    self.ip_prefix,
                    self.port,
                    self.election_in_progress,
                    self.condition,
                    self.waiting_ok,
                    self.ok_condition,
                    self.leader_id
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
                return self.leader_id.value
            self.election_in_progress.value = True

        leader = initiate_election(
            self.node_id,
            self.node_ids,
            self.ip_prefix,
            self.port,
            self.election_in_progress,
            self.condition,
            self.waiting_ok,
            self.ok_condition,
        )

        # NO es necesario esto
        # Actualiza el líder y notifica que la elección ha terminado
        with self.condition:
            self.leader_id.value = leader
            self.election_in_progress.value = False
            self.condition.notify_all()  # Notifica a otros procesos que la elección terminó
        
        return leader

    def get_leader(self):
        """Retorna el líder actual o espera a que se seleccione uno."""
        with self.condition:
            if self.leader_id.value == -1:  # Si no hay líder, espera
                self.condition.wait()  # Espera a que el listener o manager actualicen el líder
            return self.leader_id.value

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

        with self.ok_condition:
            self.waiting_ok.value = False
            self.ok_condition.notify_all()  # Notificar a los hilos en espera

        logging.info(f"ElectionManager {self.node_id}: Recursos limpiados exitosamente.")

