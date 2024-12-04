import logging
from q3_joiner_replica import Q3JoinerReplica
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q3_JOINER_REPLICA_CONFIG_KEYS


def main():
    try:
        # Inicializar configuración y logging
        config_params = initialize_config(Q3_JOINER_REPLICA_CONFIG_KEYS)
        initialize_log(config_params["logging_level"])

        # Crear una instancia de Q3JoinerReplica con un ID único
        replica = Q3JoinerReplica(
            id=config_params["instance_id"],
            container_name="q3_joiner_replica",
            container_to_restart="q3_joiner_1",
            n_replicas=config_params["q3_joiner_replica_instances"]
        )
        
        logging.info(f"Q3JoinerReplica {config_params['instance_id']} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("Q3JoinerReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"Q3JoinerReplica: Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()
