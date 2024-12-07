import logging
from os_counter_replica import OsCounterReplica
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import OS_COUNTER_REPLICA_CONFIG_KEYS


def main():
    try:
        # Inicializar configuración y logging
        config_params = initialize_config(OS_COUNTER_REPLICA_CONFIG_KEYS)
        initialize_log(config_params["logging_level"])

        # Crear una instancia de OsCounterReplica con un ID único
        replica = OsCounterReplica(
            id=config_params["instance_id"],
            container_name="os_counter_replica",
            master_name="os_counter_1",
            n_replicas=config_params["os_counter_replica_instances"]
        )
        
        logging.info(f"OsCounterReplica {config_params['instance_id']} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("OsCounterReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"OsCounterReplica: Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()