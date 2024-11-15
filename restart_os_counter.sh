#!/bin/bash

# Nombre del contenedor
CONTAINER_NAME="os_counter_1"

# Detener y eliminar el contenedor
echo "Deteniendo el contenedor $CONTAINER_NAME..."
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

# Levantar un nuevo contenedor
echo "Levantando un nuevo contenedor $CONTAINER_NAME..."
docker-compose up --no-deps -d $CONTAINER_NAME

# Mostrar logs del nuevo contenedor
docker logs -f $CONTAINER_NAME
