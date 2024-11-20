import logging
from avg_counter_replica import AvgCounterReplica
from utils.initilization import initialize_config, initialize_log

def main():
    try:

        required_keys = {
            "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL"),
            "n_instances": ("AVG_COUNTER_REPLICA_INSTANCES", "AVG_COUNTER_REPLICA_INSTANCES"),
            "id": ("INSTANCE_ID", "INSTANCE_ID")
        }

        # Inicializar configuración y logging
        config_params = initialize_config(required_keys)
        initialize_log(config_params["logging_level"])
        replica_id = config_params["id"]
        n_instances = config_params["n_instances"]
        replica = AvgCounterReplica(replica_id, n_instances)
        
        logging.info(f"AvgCounterReplica {replica_id} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("AvgCounterReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"AvgCounterReplica: Error inesperado: {e}", exc_info=True)

if __name__ == "__main__":
    main()