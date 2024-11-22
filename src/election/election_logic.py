import logging
import socket
from messages.messages import ElectionMessage, LeaderElectionMessage

PORT = 12345
TIMEOUT = 5

def initiate_election(node_id, node_ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart):
    """Inicia el algoritmo de elección Bully."""
    logging.info(f"Node {node_id}: Iniciando elección.")
    higher_node_ids = [nid for nid in node_ids if nid > node_id]

    if not higher_node_ids:
        declare_leader(node_id, node_ids, container_name, election_in_progress, condition, on_leader_selected, container_to_restart)
        return

    # Ya seteo el esperando ok antes de enviar
    with condition_ok:
        waiting_ok_election.value = True

    # Enviar mensajes de elección a nodos con ID mayor
    for nid in higher_node_ids:
        try:
            with socket.create_connection((f'{container_name}_{nid}', PORT), timeout=3) as sock:
                sock.sendall(ElectionMessage(node_id).encode())
                logging.info(f"Node {node_id}: Mensaje de elección enviado a Node {nid}.")
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")
            node_ids.remove(nid)  # Eliminar nodo inactivo
        except Exception as e:
            logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")
            node_ids.remove(nid)  # Eliminar nodo inactivo

    # Esperar OK o Timeout
    _wait_for_ok_or_leader(node_id, node_ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart)


def _wait_for_ok_or_leader(node_id, node_ids, container_name, election_in_progress, condition, waiting_ok_election, condition_ok, on_leader_selected, container_to_restart):
    """Espera una confirmación OK o una notificación de liderazgo."""
    logging.info(f"Node {node_id}: Entre a esperar los Oks")
    with condition_ok:
        # Validar si el OK ya fue recibido antes de esperar
        if not waiting_ok_election.value:
            logging.info(f"Node {node_id}: OK ya recibido antes de esperar. Continuando.")
        else:
            # Esperar por el OK con timeout
            if not condition_ok.wait_for(lambda: not waiting_ok_election.value, timeout=5):
                logging.info(f"Node {node_id}: Timeout esperando OK. Declarándose líder.")
                declare_leader(node_id, node_ids, container_name, election_in_progress, condition, on_leader_selected, container_to_restart)
                return
            logging.info(f"Node {node_id}: OK recibido. Esperando anuncio de líder.")

    # Si se recibe el OK, continuar con la lógica de esperar el anuncio del líder
    with condition:
        condition.wait_for(lambda: not election_in_progress.value)
        logging.info(f"Node {node_id}: Anuncio de líder recibido. Continuando ejecución.")



def declare_leader(node_id, node_ids, container_name, election_in_progress, condition, on_leader_selected, container_to_restart):
    """Se declara líder y ejecuta la acción proporcionada."""
    logging.info(f"Node {node_id}: Soy el nuevo líder.")

    # Ejecutar la acción personalizada para el líder
    try:
        on_leader_selected(container_to_restart)
    except Exception as e:
        logging.error(f"Node {node_id}: Error ejecutando acción como líder: {e}")

    # Notificar a los nodos que es el líder
    _notify_leader_selected(node_id, node_ids, container_name, election_in_progress, condition)


def _notify_leader_selected(node_id, node_ids, container_name, election_in_progress, condition):
    """Notifica a todos los nodos que este nodo es el líder."""
    for nid in node_ids:
        if nid != node_id:
            try:
                with socket.create_connection((f'{container_name}_{nid}', PORT), timeout=TIMEOUT) as sock:
                    sock.sendall(LeaderElectionMessage(node_id).encode())
                    logging.info(f"Node {node_id}: Notificación de liderazgo enviada a Node {nid}.")
            except (ConnectionRefusedError, socket.timeout, socket.gaierror):
                logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")
            except Exception as e:
                logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")

    with condition:
        election_in_progress.value = False
        condition.notify_all()  # Notificar a otros procesos que la elección terminó
    logging.info(f"Node {node_id}: Elección finalizada, election_in_progress=False.")
