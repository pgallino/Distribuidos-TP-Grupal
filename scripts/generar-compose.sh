#!/bin/bash

# Obtener la ruta del directorio donde se encuentra este script
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Cargar las variables desde el archivo config.env
CONFIG_FILE="$SCRIPT_DIR/config.env"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Archivo de configuración '$CONFIG_FILE' no encontrado."
    exit 1
fi

# Exportar las variables definidas en config.env
set -a
source "$CONFIG_FILE"
set +a

# Lista de variables requeridas
VARIABLES=(
    trimmer genre score release_date english client
    os_counter_replica avg_counter_replica
    q3_joiner_replica q4_joiner_replica q5_joiner_replica watchdog
)

# Verificar que todas las variables estén definidas
for var in "${VARIABLES[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo "Error: Falta la configuración para '$var' en '$CONFIG_FILE'."
        exit 1
    fi
done

# Construir una cadena de argumentos para el script de Python
ARGS=()
for var in "${VARIABLES[@]}"; do
    ARGS+=("${!var}")
done

# Ejecuta el script de Python con los argumentos
python3 "$SCRIPT_DIR/script-generar-compose.py" "${ARGS[@]}"
