import logging
from avg_counter_replica import AvgCounterReplica
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import AVG_COUNTER_REPLICA_CONFIG_KEYS


def main():
    try:
        # Inicializar configuración y logging
        config_params = initialize_config(AVG_COUNTER_REPLICA_CONFIG_KEYS)
        initialize_log(config_params["logging_level"])

        # Crear una instancia de AvgCounterReplica con un ID único
        replica = AvgCounterReplica(
            id=config_params["instance_id"],
            container_name="avg_counter_replica",
            master_name="avg_counter_1",
            n_replicas=config_params["avg_counter_replica_instances"]
        )
        
        logging.info(f"AvgCounterReplica {config_params['instance_id']} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("AvgCounterReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"AvgCounterReplica: Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()