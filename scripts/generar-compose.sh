#!/bin/bash

# Obtener la ruta del directorio donde se encuentra este script
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Cargar las variables desde el archivo .env
CONFIG_FILE="$SCRIPT_DIR/config.env"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Error: Archivo de configuración '$CONFIG_FILE' no encontrado."
    exit 1
fi
# Exportar las variables definidas en config.env
set -a
source "$CONFIG_FILE"
set +a

# Verificar que todas las variables estén presentes
for nodo in trimmer genre score release_date english client os_counter_replica avg_counter_replica q3_joiner_replica; do
    if [[ -z "${!nodo}" ]]; then
        echo "Error: Falta la configuración para $nodo en '$CONFIG_FILE'."
        exit 1
    fi
done

# Ejecuta el script de Python con los parámetros
python3 "$SCRIPT_DIR/script-generar-compose.py" "$trimmer" "$genre" "$score" "$release_date" "$english" "$client" "$os_counter_replica" "$avg_counter_replica" "$q3_joiner_replica"
