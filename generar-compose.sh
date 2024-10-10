#!/bin/bash

# Inicializa un array para las instancias
declare -A INSTANCIAS

# Procesa los parámetros
for arg in "$@"; do
    IFS='=' read -r nodo valor <<< "$arg"
    INSTANCIAS["$nodo"]="$valor"
done

# Verifica que todos los nodos requeridos estén presentes
for nodo in trimmer genre score release_date english os_counter average_counter; do
    if [[ -z "${INSTANCIAS[$nodo]}" ]]; then
        echo "Error: Falta el parámetro para $nodo"
        exit 1
    fi
done

# Ejecuta el script de Python con los parámetros
python3 script-generar-compose.py "${INSTANCIAS[trimmer]}" "${INSTANCIAS[genre]}" "${INSTANCIAS[score]}" "${INSTANCIAS[release_date]}" "${INSTANCIAS[english]}" "${INSTANCIAS[os_counter]}" "${INSTANCIAS[average_counter]}"

#./generar-compose.sh trimmer=2 genre=3 score=1 release_date=4 english=5 os_counter=2 average_counter=1