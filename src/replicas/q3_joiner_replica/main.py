import logging
from q3_joiner_replica import Q3JoinerReplica
from utils.initilization import initialize_config, initialize_log

def main():
    try:

        required_keys = {
            "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
            "n_instances": ("Q3_JOINER_REPLICA_INSTANCES", "Q3_JOINER_REPLICA_INSTANCES"),
            "id": ("INSTANCE_ID", "INSTANCE_ID"),
            "timeout": ("TIMEOUT", "TIMEOUT"),
            "port": ("PORT", "PORT")
        }
        # Inicializar configuración y logging
        config_params = initialize_config(required_keys)
        initialize_log(config_params["logging_level"])
        # Crear una instancia de Q3JoinerReplica con un ID único
        replica_id = config_params["id"]
        n_instances = config_params["n_instances"]
        timeout = config_params["timeout"]
        port = config_params["port"]
        replica = Q3JoinerReplica(replica_id, n_instances, "q3_joiner_replica", port, "q3_joiner_1", timeout)
        
        logging.info(f"Q3JoinerReplica {replica_id} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("Q3JoinerReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"Q3JoinerReplica: Error inesperado: {e}", exc_info=True)

if __name__ == "__main__":
    main()
