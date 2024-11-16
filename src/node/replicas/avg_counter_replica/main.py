import logging
from avg_counter_replica import AvgCounterReplica
from utils.initilization import initialize_config, initialize_log

def main():
    try:

        required_keys = {
            "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
        }

        # Inicializar configuración y logging
        config_params = initialize_config(required_keys)
        initialize_log(config_params["logging_level"])
        # Crear una instancia de AvgCounterReplica con un ID único
        replica_id = 1  # Cambia esto según sea necesario
        replica = AvgCounterReplica(id=replica_id)
        
        logging.info(f"AvgCounterReplica {replica_id} iniciada. Esperando mensajes...")
        
        # Ejecutar la réplica
        replica.run()
    except KeyboardInterrupt:
        logging.info("AvgCounterReplica: Interrumpida manualmente. Cerrando...")
    except Exception as e:
        logging.error(f"AvgCounterReplica: Error inesperado: {e}", exc_info=True)

if __name__ == "__main__":
    main()