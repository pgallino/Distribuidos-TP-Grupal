#!/bin/bash

# Cantidad de pruebas
num_tests=20

# Directorios donde se guardarán los resultados
results_dir="./results/results_client_1"  # Ruta relativa desde ./tests/
reference_dir="./results_client_1"  # Ruta relativa desde ./tests/

# Limpiar el directorio de resultados antes de ejecutar el programa
echo "Verificando existencia del directorio $results_dir..."
if [ -d "$results_dir" ]; then
  echo "Eliminando directorio $results_dir..."
  rm -rf "$results_dir"
  if [ $? -ne 0 ]; then
    echo "Error al eliminar el directorio $results_dir. Verifica los permisos."
    exit 1
  else
    echo "Directorio $results_dir eliminado correctamente."
  fi
else
  echo "El directorio $results_dir no existe. No es necesario eliminarlo."
fi

# Verificar si el directorio fue eliminado correctamente
if [ -d "$results_dir" ]; then
  echo "El directorio $results_dir no fue eliminado correctamente. Verifica si hay procesos bloqueando el directorio."
else
  echo "El directorio $results_dir fue eliminado correctamente."
fi

# Crear el directorio limpio
echo "Creando directorio $results_dir..."
mkdir -p "$results_dir"
echo "Directorio $results_dir listo."

# Array para almacenar los resultados de cada prueba
declare -a results

# Función para manejar Ctrl+C
handle_sigint() {
  echo
  echo "⚠️  Ctrl+C detectado. Limpiando contenedores y cerrando..."
  make docker-compose-down
  echo "Contenedores detenidos. Saliendo del script."
# Mostrar resumen de resultados
  echo "========================================"
  echo "Resumen de resultados"
  echo "========================================"
  for ((i=1; i<=num_tests; i++)); do
    echo "${results[$i]}"
  done
  exit 1
}

# Configurar la captura de SIGINT (Ctrl+C)
trap handle_sigint SIGINT

# Función para verificar si se han generado los archivos Q1.txt, Q2.txt, ..., Q5.txt
wait_for_files() {
  local target_dir="$1"
  local expected_files=("Q1.txt" "Q2.txt" "Q3.txt" "Q4.txt" "Q5.txt")  # Archivos específicos a esperar

  echo "Esperando a que se generen los archivos Q1.txt, Q2.txt, Q3.txt, Q4.txt y Q5.txt en $target_dir..."

  # Verificar si todos los archivos esperados están presentes
  while true; do
    missing_files=0
    for file in "${expected_files[@]}"; do
      if [ ! -f "$target_dir/$file" ]; then
        missing_files=$((missing_files+1))
      fi
    done

    # Si no faltan archivos, salimos del bucle
    if [ "$missing_files" -eq 0 ]; then
      echo " Todos los archivos esperados fueron generados en $target_dir."
      break
    fi

    sleep 5
  done
}

for ((i=1; i<=num_tests; i++))
do
  echo "Ejecutando prueba $i..."

  # Crear el subdirectorio para logs y diff
  test_dir="./restultados_replicas/test_$i"
  mkdir -p "$test_dir/logs"  # Crear el subdirectorio para logs
  mkdir -p "$test_dir/diffs"  # Crear el subdirectorio para los resultados de diff
  echo "Subdirectorios $test_dir/logs y $test_dir/diffs creados."

  # Archivo de log para esta prueba
  log_file="$test_dir/logs/logs_prueba_$i.log"  # Ajustado para estar en el subdirectorio correcto

  # Levantar el sistema y redirigir logs
  make docker-compose-up-logs > "$log_file" 2>&1 &

  UP_PID=$!

  # Asegurarse de que el archivo de logs exista antes de buscar en él
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

  # Esperar a que los archivos Q1.txt, Q2.txt, Q3.txt, Q4.txt y Q5.txt sean generados
  wait_for_files "$results_dir"  # Ahora espera los archivos Q1.txt, Q2.txt, Q3.txt, Q4.txt, Q5.txt

  # Detener el sistema
  echo "Prueba $i: deteniendo contenedores..."
  make docker-compose-down

  # Realizar el diff entre los directorios
  echo "Realizando la comparación entre los directorios..."
  diff -r "$results_dir" "$reference_dir" > "$test_dir/diffs/diff_output_prueba_$i.txt"

  # Capturamos el resultado del diff
  if [ $? -eq 0 ]; then
    # No hay diferencias
    results[$i]="Prueba $i: ✅ No hay diferencias encontradas"
  else
    # Se encontraron diferencias
    results[$i]="Prueba $i: ❌ Se encontraron diferencias"
  fi

  # Limpiar el directorio de resultados antes de ejecutar el programa
  echo "Verificando existencia del directorio $results_dir..."
  if [ -d "$results_dir" ]; then
    echo "Eliminando directorio $results_dir..."
    rm -rf "$results_dir"
    if [ $? -ne 0 ]; then
      echo "Error al eliminar el directorio $results_dir. Verifica los permisos."
      exit 1
    else
      echo "Directorio $results_dir eliminado correctamente."
    fi
  else
    echo "El directorio $results_dir no existe. No es necesario eliminarlo."
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
echo "Archivos 'diff' generados"
echo "========================================"
for ((i=1; i<=num_tests; i++)); do
  echo "Resultado de prueba $i: diff_output_prueba_$i.txt"
done
echo "======================"
