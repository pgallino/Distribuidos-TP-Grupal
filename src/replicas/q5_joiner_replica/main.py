import logging
from q5_joiner_replica import Q5JoinerReplica
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q5_JOINER_REPLICA_CONFIG_KEYS


def main():
    try:
        # Inicializar configuración y logging
        config_params = initialize_config(Q5_JOINER_REPLICA_CONFIG_KEYS)
        initialize_log(config_params["logging_level"])

        # Crear una instancia de Q5JoinerReplica con un ID único
        replica = Q5JoinerReplica(
            id=config_params["instance_id"],
            container_name="q5_joiner_replica",
            master_name="q5_joiner_1",
            n_replicas=config_params["q5_joiner_replica_instances"]
        )
        
        logging.info(f"Q5JoinerReplica {config_params['instance_id']} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("Q5JoinerReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"Q5JoinerReplica: Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()