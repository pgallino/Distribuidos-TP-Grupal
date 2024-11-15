#!/bin/bash

# testear con estos datasets cortos asi no rompe https://drive.google.com/drive/folders/1Oqcfio45qJbm07X3Ks3lup3A9c42F1HM?usp=sharing

# Cantidad de pruebas
num_tests=20

# Carpeta donde se guardarán los resultados
results_dir="client_test_results"
mkdir -p "$results_dir"

# Array para almacenar los resultados de cada prueba
declare -a results

# Mensajes esperados en los logs
expected_messages=(
  "Q1: OS Count Summary:"
  "Q2: Names of the top 10 'Indie' genre games of the 2010s with the highest average historical playtime:"
  "Q3: Top 5 Indie Games with Most Positive Reviews:"
  "Q4: Action games with more than 5,000 negative reviews in English:"
  "Q5: Games in the 90th Percentile for Negative Reviews (Action Genre):"
)

# Función para manejar Ctrl+C
handle_sigint() {
  echo
  echo "⚠️  Ctrl+C detectado. Limpiando contenedores y cerrando..."
  make docker-compose-down
  echo "Contenedores detenidos. Saliendo del script."
  exit 1
}

# Configurar la captura de SIGINT (Ctrl+C)
trap handle_sigint SIGINT

# Función para detectar el número de clientes automáticamente
detect_clients() {
  local log_file=$1
  grep -oP 'Container client_\d+' "$log_file" | sort -u | wc -l
}

for ((i=1; i<=num_tests; i++))
do
  echo "Ejecutando prueba $i..."

  # Archivo de log para esta prueba
  log_file="$results_dir/logs_prueba_$i.log"

  # Levantar el sistema y redirigir logs
  make docker-compose-up-logs > "$log_file" 2>&1 &
  UP_PID=$!

  # Asegurarse de que el archivo de logs existe antes de buscar en él
  echo "Esperando a que se cree el archivo de logs..."
  while [ ! -f "$log_file" ]; do
    sleep 1
  done

  # Detectar el número de clientes
  echo "Detectando número de clientes..."
  while true; do
    max_clients=$(detect_clients "$log_file")
    if [ "$max_clients" -gt 0 ]; then
      echo "Número de clientes detectado: $max_clients"
      break
    fi
    sleep 1
  done

  # Esperar hasta que el programa haya arrancado completamente
  echo "Esperando a que el programa arranque..."
  until grep -q "docker compose -f docker-compose-dev.yaml logs -f" "$log_file"; do
    sleep 1
  done
  echo "Programa arrancado para la prueba $i."

  # Esperar hasta que todos los clientes hayan generado los 5 mensajes esperados
  echo "Esperando a que los clientes generen los 5 mensajes esperados..."
  start_time=$(date +%s)
  success=true

  while true; do
    all_messages_found=true

    # Verificar para cada cliente
    for client_id in $(seq 1 $max_clients); do
      client_logs=$(grep "client_$client_id" "$log_file")

      for msg in "${expected_messages[@]}"; do
        if ! echo "$client_logs" | grep -q "$msg"; then
          all_messages_found=false
          break
        fi
      done

      if [ "$all_messages_found" = false ]; then
        break
      fi
    done

    # Verificar si todos los mensajes fueron encontrados
    if [ "$all_messages_found" = true ]; then
      echo "✅ Todos los mensajes esperados encontrados en los logs."
      break
    fi

    # Salir si excede los 5 segundos
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    if [ $elapsed -ge 5 ]; then
      echo "⚠️ No se encontraron todos los mensajes esperados en 5 segundos."
      success=false
      break
    fi

    sleep 1
  done

  # Detener el sistema
  echo "Prueba $i: deteniendo contenedores..."
  make docker-compose-down

  # Guardar resultado de la prueba
  if [ "$success" = true ]; then
    results[$i]="Prueba $i: ✅ Mensajes encontrados"
  else
    results[$i]="Prueba $i: ❌ Faltan mensajes esperados"
  fi

done

# Mostrar resumen de resultados
echo "========================================"
echo "Resumen de resultados"
echo "========================================"
for ((i=1; i<=num_tests; i++)); do
  echo "${results[$i]}"
done
echo "======================"
