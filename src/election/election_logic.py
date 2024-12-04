import logging
import socket
from messages.messages import MsgType, SimpleMessage
from utils.container_constants import LISTENER_PORT

TIMEOUT = 5

def initiate_election(node_id, node_ids: list[int], ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id):
    """Inicia el algoritmo de elección Bully."""
    logging.info(f"Node {node_id}: Iniciando elección.")
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
                logging.info(f"Node {node_id}: Mensaje de elección enviado a Node {ip_prefix}_{nid}:{LISTENER_PORT}.")
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")

        except Exception as e:
            logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")

    # Esperar OK o Timeout
    _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id)


def _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, waiting_ok, ok_condition, leader_id):
    """Espera una confirmación OK o una notificación de liderazgo."""
    logging.info(f"Node {node_id}: Entre a esperar los Oks")
    with ok_condition:
        # Validar si el OK ya fue recibido antes de esperar
        if not waiting_ok.value:
            logging.info(f"Node {node_id}: OK ya recibido antes de esperar. Continuando.")
        else:
            # Esperar por el OK con timeout
            if not ok_condition.wait_for(lambda: not waiting_ok.value, timeout=20):
                logging.info(f"Node {node_id}: Timeout esperando OK. Declarándose líder.")
                declare_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id)
                return
            logging.info(f"Node {node_id}: OK recibido. Esperando anuncio de líder.")

    # Si se recibe el OK, continuar con la lógica de esperar el anuncio del líder
    with election_condition:
        election_condition.wait_for(lambda: not election_in_progress.value)
        logging.info(f"Node {node_id}: Anuncio de líder recibido. Continuando ejecución.")

def declare_leader(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id):
    """Se declara líder y ejecuta la acción proporcionada."""
    logging.info(f"Node {node_id}: Soy el nuevo líder.")

    with election_condition:
        leader_id.value = node_id

    # Notificar a los nodos que es el líder
    _notify_leader_selected(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id)

def _notify_leader_selected(node_id, node_ids, ip_prefix, election_in_progress, election_condition, leader_id):
    """Notifica a todos los nodos que este nodo es el líder."""
    for nid in node_ids:
        if nid != node_id:
            try:
                with socket.create_connection((f'{ip_prefix}_{nid}', LISTENER_PORT), timeout=TIMEOUT) as sock:
                    leader_msg = SimpleMessage(type=MsgType.LEADER_ELECTION, socket_compatible=True, node_id=node_id)
                    sock.sendall(leader_msg.encode())
                    logging.info(f"Node {node_id}: Notificación de liderazgo enviada a Node {nid}.")
            except (ConnectionRefusedError, socket.timeout, socket.gaierror):
                logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")
            except Exception as e:
                logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")

    with election_condition:
        election_in_progress.value = False
        election_condition.notify_all()  # Notificar a otros procesos que la elección terminó
    logging.info(f"Node {node_id}: Elección finalizada, election_in_progress=False.")
