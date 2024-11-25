import logging
from os_counter_replica import OsCounterReplica
from utils.initilization import initialize_config, initialize_log

def main():
    try:

        required_keys = {
            "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
            "n_instances": ("OS_COUNTER_REPLICA_INSTANCES", "OS_COUNTER_REPLICA_INSTANCES"),
            "id": ("INSTANCE_ID", "INSTANCE_ID"),
            "timeout": ("TIMEOUT", "TIMEOUT")
        }

        # Inicializar configuración y logging
        config_params = initialize_config(required_keys)
        initialize_log(config_params["logging_level"])
        # Crear una instancia de OsCounterReplica con un ID único
        replica_id = config_params["id"]
        n_instances = config_params["n_instances"]
        timeout = config_params["timeout"]
        replica = OsCounterReplica(replica_id, n_instances, "os_counter_replica", "os_counter_1", timeout)
        
        logging.info(f"OsCounterReplica {replica_id} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("OsCounterReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"OsCounterReplica: Error inesperado: {e}", exc_info=True)

if __name__ == "__main__":
    main()
