#!/bin/bash

# Cantidad de pruebas
num_tests=20

# Carpeta donde se guardarán los resultados
results_dir="tests/q5_reviews_test"
mkdir -p "$results_dir"

# Array para almacenar los resultados de cada prueba
declare -a results

# Array para almacenar los números extraídos
declare -a extracted_numbers

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

  # Levantar el sistema y redirigir logs
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

  # Buscar la línea esperada en los logs
  echo "Buscando la línea en los logs del contenedor q5_joiner_1..."
  success=false
  number="N/A"

  while true; do
    line=$(grep "q5_joiner_1" "$log_file" | grep -oP "LA LONGITUD DE REVIEWS ES: \K\d+")
    if [ -n "$line" ]; then
      echo "✅ Línea encontrada: LA LONGITUD DE REVIEWS ES: $line"
      number=$line
      success=true
      break
    fi

    # Salir si termina el proceso principal
    if ! ps -p $UP_PID > /dev/null; then
      echo "⚠️ El sistema se detuvo sin encontrar la línea esperada."
      break
    fi

    sleep 1
  done

  # Detener el sistema
  echo "Prueba $i: deteniendo contenedores..."
  make docker-compose-down

  # Guardar resultado de la prueba y número extraído
  if [ "$success" = true ]; then
    results[$i]="Prueba $i: ✅ Línea encontrada con número $number"
    extracted_numbers[$i]=$number
  else
    results[$i]="Prueba $i: ❌ Línea no encontrada"
    extracted_numbers[$i]="N/A"
  fi

done

# Mostrar resumen de resultados
echo "========================================"
echo "Resumen de resultados"
echo "========================================"
for ((i=1; i<=num_tests; i++)); do
  echo "${results[$i]}"
done

echo "========================================"
echo "Números extraídos"
echo "========================================"
for ((i=1; i<=num_tests; i++)); do
  echo "Prueba $i: ${extracted_numbers[$i]}"
done
echo "======================"
