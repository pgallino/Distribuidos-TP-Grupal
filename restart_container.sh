#!/bin/bash

# Verificar si se pasó un argumento
if [ $# -eq 0 ]; then
    echo "Error: Debes proporcionar el nombre del contenedor."
    echo "Uso: $0 <container_name>"
    exit 1
fi

# Nombre del contenedor recibido como argumento
CONTAINER_NAME="$1"

# Detener y eliminar el contenedor
echo "Deteniendo el contenedor $CONTAINER_NAME..."
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

# Levantar un nuevo contenedor
echo "Levantando un nuevo contenedor $CONTAINER_NAME..."
docker-compose -f docker-compose-dev.yaml up --no-deps -d $CONTAINER_NAME