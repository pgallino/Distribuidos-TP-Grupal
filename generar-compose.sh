#!/bin/bash

# Verificar que la cantidad de parámetros sea la adecuada
if [ "$#" -ne 2 ]; then
    echo "Por favor ejecute con los parámetros: $0 <nombre-del-archivo-de-salida> <cantidad-de-clientes>"
    exit 1
fi

echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"

output_file=$1
client_count=$2

cat <<EOF > $output_file
name: steamyanalytics
services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:latest
    ports:
      - "5672:5672" # Puerto para conexiones AMQP
      - "15672:15672" # Puerto para la consola de administración (opcional)
    networks:
      - testing_net

  server:
    container_name: server
    image: server:latest
    volumes:
      - ./server/config.ini:/config.ini
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - rabbitmq
EOF

for i in $(seq 1 $client_count); do
    cat <<EOF >> $output_file

  client$i:
    container_name: client$i
    image: client:latest
    networks:
      - testing_net
    depends_on:
      - rabbitmq
EOF
done

cat <<EOF >> $output_file

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOF

echo "Archivo $output_file generado con $client_count clientes."
