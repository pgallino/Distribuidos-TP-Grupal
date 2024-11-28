import logging
from q5_joiner_replica import Q5JoinerReplica
from utils.initilization import initialize_config, initialize_log

def main():
    try:

        required_keys = {
            "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
            "n_instances": ("Q5_JOINER_REPLICA_INSTANCES", "Q5_JOINER_REPLICA_INSTANCES"),
            "id": ("INSTANCE_ID", "INSTANCE_ID"),
            "timeout": ("TIMEOUT", "TIMEOUT"),
            "port": ("PORT", "PORT")
        }

        # Inicializar configuración y logging
        config_params = initialize_config(required_keys)
        initialize_log(config_params["logging_level"])
        # Crear una instancia de Q5JoinerReplica con un ID único
        replica_id = config_params["id"]
        n_instances = config_params["n_instances"]
        timeout = config_params["timeout"]
        port = config_params["port"]
        replica = Q5JoinerReplica(replica_id, n_instances, "q5_joiner_replica", port, "q5_joiner_1", timeout)
        
        logging.info(f"Q5JoinerReplica {replica_id} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("Q5JoinerReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"Q5JoinerReplica: Error inesperado: {e}", exc_info=True)

if __name__ == "__main__":
    main()
