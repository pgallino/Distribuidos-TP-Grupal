#!/bin/bash

# Cantidad de pruebas
num_tests=20

# Carpeta donde se guardarán los resultados
results_dir="shutdown_test_results"
mkdir -p "$results_dir"

# Array para almacenar los resultados de cada prueba
declare -a results

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

for ((i=1; i<=num_tests; i++))
do
  echo "Ejecutando prueba $i..."

  # Archivo de log para esta prueba
  log_file="$results_dir/logs_prueba_$i.log"

  # Levantar el programa y redirigir logs
  make docker-compose-up-logs > "$log_file" 2>&1 &
  UP_PID=$!

  # Asegurarse de que el archivo de logs existe antes de buscar en él
  echo "Esperando a que se cree el archivo de logs..."
  while [ ! -f "$log_file" ]; do
    sleep 1
  done

  # Esperar hasta que el programa haya arrancado completamente
  echo "Esperando a que el programa arranque..."
  until grep -q "docker compose -f docker-compose-dev.yaml logs -f" "$log_file"; do
    sleep 1
  done
  echo "Programa arrancado para la prueba $i."

  # Esperar un tiempo aleatorio entre 2 y 60 segundos desde que el programa arrancó
  wait_time=$((RANDOM % 59 + 2))
  echo "Prueba $i: esperando $wait_time segundos para shutdown..."
  sleep $wait_time

  # Detener los contenedores
  echo "Prueba $i: deteniendo contenedores..."
  if ! make docker-compose-down; then
    echo "⚠️  Error al detener los contenedores en la prueba $i."
    results[$i]="Prueba $i: ❌ Error al detener contenedores"
    continue
  fi

  # Esperar a que el proceso termine
  wait $UP_PID

  # Analizar los logs en busca de errores o códigos de salida problemáticos
  echo "Analizando logs de prueba $i..."
  if grep -qE "exited with code (137|1|[2-9][0-9]+)" "$log_file"; then
    echo "⚠️  Se encontraron errores en los logs de la prueba $i."
    grep "exited with code" "$log_file"
    results[$i]="Prueba $i: ❌ Con errores"
  else
    echo "✅ Prueba $i completada sin errores."
    results[$i]="Prueba $i: ✅ Sin errores"
  fi

  echo "Prueba $i finalizada. Logs guardados en $log_file"
done

# Mostrar resumen de resultados
echo "========================================"
echo "Resumen de resultados"
echo "========================================"
for ((i=1; i<=num_tests; i++)); do
  echo "${results[$i]}"
done
echo "======================"
