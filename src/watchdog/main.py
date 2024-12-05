from utils.container_constants import WATCHDOG_CONFIG_KEYS, WATCHDOG_CONTAINER_NAME, WATCHDOG_NODES_TO_MONITOR
from utils.initilization import initialize_config, initialize_log
from watchdog import WatchDog

def main():
    # Generar la lista completa de claves necesarias para la configuración
    required_keys = WATCHDOG_CONFIG_KEYS

    # Inicializar la configuración
    config_params = initialize_config(required_keys)
    instance_id = config_params["instance_id"]
    n_nodes = config_params[f"{WATCHDOG_CONTAINER_NAME}_instances"]

    # Configurar logging
    initialize_log(config_params["logging_level"])

    # Crear lista de tuplas para n_nodes_instances
    nodes_to_monitor = [
        (node, config_params[f"{node}_instances"])
        for node in WATCHDOG_NODES_TO_MONITOR
    ]

    # Crear una instancia de WatchDog
    watchdog = WatchDog(
        id=instance_id,
        n_watchdogs=n_nodes,
        container_name=WATCHDOG_CONTAINER_NAME,
        nodes_to_monitor=nodes_to_monitor,
    )

    # Ejecutar el WatchDog
    watchdog.run()

if __name__ == "__main__":
    main()
