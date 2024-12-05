import logging
import signal
import socket
from messages.messages import MsgType, SimpleMessage
from utils.container_constants import LISTENER_PORT

TIMEOUT = 5

def initiate_election(node_id, node_ids: list[int], ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id):
    """Inicia el algoritmo de elección Bully."""

    def handle_sigterm(sig, frame):
        exit(0)
    signal.signal(signal.SIGTERM, handle_sigterm)

    logging.info(f"[Manager] Iniciando elección.")
    higher_node_ids = [nid for nid in node_ids if nid > node_id]

    if not higher_node_ids:
        declare_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id)
        return

    # Ya seteo el esperando ok antes de enviar
    with ok_condition:
        waiting_ok.value = True

    # Enviar mensajes de elección a nodos con ID mayor
    for nid in higher_node_ids:
        try:
            with socket.create_connection((f'{ip_prefix}_{nid}', LISTENER_PORT), timeout=3) as sock:
                e_msg = SimpleMessage(type=MsgType.ELECTION, socket_compatible=True, node_id=node_id)
                sock.sendall(e_msg.encode())
                logging.info(f"[Manager] Mensaje de elección enviado a Node {ip_prefix}_{nid}:{LISTENER_PORT}.")
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"[Manager] No se pudo contactar a Node {nid} (timeout o nodo caido).")

        except Exception as e:
            logging.error(f"[Manager] Error inesperado al contactar a Node {nid}: {e}")

    # Esperar OK o Timeout
    _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id)


def _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id):
    """Espera una confirmación OK o una notificación de liderazgo."""
    with ok_condition:
        # logging.info(f"[Manager] Entre a esperar los Oks")
        # Validar si el OK ya fue recibido antes de esperar
        if not waiting_ok.value:
            logging.info(f"[Manager] OK ya recibido antes de esperar. Continuando.")
        else:
            # Esperar por el OK con timeout
            if not ok_condition.wait_for(lambda: not waiting_ok.value, timeout=10):
                logging.info(f"[Manager] Timeout esperando OK. Declarándose líder.")
                declare_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id)
                return
            logging.info(f"[Manager] OK recibido. Esperando anuncio de líder.")

    # Si se recibe el OK, continuar con la lógica de esperar el anuncio del líder
    with election_condition:
        election_condition.wait_for(lambda: not election_in_progress.value)
        # logging.info(f"[Manager] Anuncio de líder recibido. Continuando ejecución.")

def declare_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id):
    """Se declara líder y ejecuta la acción proporcionada."""
    logging.info(f"[Manager] Soy el nuevo líder.")

    with election_condition:
        leader_id.value = node_id

    # logging.info(f"[Manager] Entro a notificar que soy el leader.")
    # Notificar a los nodos que es el líder
    _notify_leader_selected(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id)

def _notify_leader_selected(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id):
    """Notifica a todos los nodos que este nodo es el líder."""
    for nid in node_ids:
        if nid != node_id:
            try:
                with socket.create_connection((f'{ip_prefix}_{nid}', LISTENER_PORT), timeout=TIMEOUT) as sock:
                    leader_msg = SimpleMessage(type=MsgType.COORDINATOR, socket_compatible=True, node_id=node_id)
                    sock.sendall(leader_msg.encode())
                    # logging.info(f"[Manager] Notificación de liderazgo enviada a Node {nid}.")
            except (ConnectionRefusedError, socket.timeout, socket.gaierror):
                logging.warning(f"[Manager] No se pudo contactar a Node {nid} (timeout o nodo caido).")
            except Exception as e:
                logging.error(f"[Manager] Error inesperado al contactar a Node {nid}: {e}")

    with election_condition:
        election_in_progress.value = False
        election_condition.notify_all()  # Notificar a otros procesos que la elección terminó
    logging.info(f"[Manager] Elección finalizada, election_in_progress=False.")
