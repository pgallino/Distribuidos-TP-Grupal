import logging
import socket
from messages.messages import MsgType, SimpleMessage

TIMEOUT = 5

def initiate_election(node_id, node_ids, ip_prefix, port, election_in_progress, condition, waiting_ok, ok_condition):
    """Inicia el algoritmo de elección Bully."""
    logging.info(f"Node {node_id}: Iniciando elección.")
    higher_node_ids = [nid for nid in node_ids if nid > node_id]

    if not higher_node_ids:
        return declare_leader(node_id, node_ids, ip_prefix, port, election_in_progress, condition)

    # Ya seteo el esperando ok antes de enviar
    with ok_condition:
        waiting_ok.value = True

    # Enviar mensajes de elección a nodos con ID mayor
    for nid in higher_node_ids:
        try:
            with socket.create_connection((f'{ip_prefix}_{nid}', port), timeout=3) as sock:
                e_msg = SimpleMessage(type=MsgType.ELECTION, socket_compatible=True, node_id=node_id)
                sock.sendall(e_msg.encode())
                logging.info(f"Node {node_id}: Mensaje de elección enviado a Node {nid}.")
        except (ConnectionRefusedError, socket.timeout, socket.gaierror):
            logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")
            node_ids.remove(nid)  # Eliminar nodo inactivo
        except Exception as e:
            logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")
            node_ids.remove(nid)  # Eliminar nodo inactivo

    # Esperar OK o Timeout
    return _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, port, election_in_progress, condition, waiting_ok, ok_condition)


def _wait_for_ok_or_leader(node_id, node_ids, ip_prefix, port, election_in_progress, condition, waiting_ok, ok_condition):
    """Espera una confirmación OK o una notificación de liderazgo."""
    logging.info(f"Node {node_id}: Entre a esperar los Oks")
    with ok_condition:
        # Validar si el OK ya fue recibido antes de esperar
        if not waiting_ok.value:
            logging.info(f"Node {node_id}: OK ya recibido antes de esperar. Continuando.")
        else:
            # Esperar por el OK con timeout
            if not ok_condition.wait_for(lambda: not waiting_ok.value, timeout=5):
                logging.info(f"Node {node_id}: Timeout esperando OK. Declarándose líder.")
                declare_leader(node_id, node_ids, ip_prefix, port, election_in_progress, condition)
                return
            logging.info(f"Node {node_id}: OK recibido. Esperando anuncio de líder.")

    # Si se recibe el OK, continuar con la lógica de esperar el anuncio del líder
    with condition:
        condition.wait_for(lambda: not election_in_progress.value)
        logging.info(f"Node {node_id}: Anuncio de líder recibido. Continuando ejecución.")

    # Retornar el nodo líder y los nodos vivos
    leader_id = max(node_ids)
    return leader_id



def declare_leader(node_id, node_ids, ip_prefix, port, election_in_progress, condition):
    """Se declara líder y ejecuta la acción proporcionada."""
    logging.info(f"Node {node_id}: Soy el nuevo líder.")

    # Notificar a los nodos que es el líder
    _notify_leader_selected(node_id, node_ids, ip_prefix, port, election_in_progress, condition)
    return node_id


def _notify_leader_selected(node_id, node_ids, ip_prefix, port, election_in_progress, condition):
    """Notifica a todos los nodos que este nodo es el líder."""
    for nid in node_ids:
        if nid != node_id:
            try:
                with socket.create_connection((f'{ip_prefix}_{nid}', port), timeout=TIMEOUT) as sock:
                    leader_msg = SimpleMessage(type=MsgType.LEADER_ELECTION, socket_compatible=True, id=node_id)
                    sock.sendall(leader_msg.encode())
                    logging.info(f"Node {node_id}: Notificación de liderazgo enviada a Node {nid}.")
            except (ConnectionRefusedError, socket.timeout, socket.gaierror):
                logging.warning(f"Node {node_id}: No se pudo contactar a Node {nid} (timeout o nodo caido).")
            except Exception as e:
                logging.error(f"Node {node_id}: Error inesperado al contactar a Node {nid}: {e}")

    with condition:
        election_in_progress.value = False
        condition.notify_all()  # Notificar a otros procesos que la elección terminó
    logging.info(f"Node {node_id}: Elección finalizada, election_in_progress=False.")
