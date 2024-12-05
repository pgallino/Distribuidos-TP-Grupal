import logging
from q4_joiner_replica import Q4JoinerReplica
from utils.initilization import initialize_config, initialize_log
from utils.container_constants import Q4_JOINER_REPLICA_CONFIG_KEYS


def main():
    try:
        # Inicializar configuración y logging
        config_params = initialize_config(Q4_JOINER_REPLICA_CONFIG_KEYS)
        initialize_log(config_params["logging_level"])

        # Crear una instancia de Q4JoinerReplica con un ID único
        replica = Q4JoinerReplica(
            id=config_params["instance_id"],
            container_name="q4_joiner_replica",
            container_to_restart="q4_joiner_1",
        )
        
        logging.info(f"Q4JoinerReplica {config_params['instance_id']} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("Q4JoinerReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"Q4JoinerReplica: Error inesperado: {e}", exc_info=True)


if __name__ == "__main__":
    main()

