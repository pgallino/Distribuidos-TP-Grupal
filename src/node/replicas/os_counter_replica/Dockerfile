FROM python:3.9.7-slim

# Instalar Docker CLI y solo las dependencias esenciales
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY src/node/replicas/os_counter_replica/main.py /
COPY src/node/replicas/os_counter_replica/os_counter_replica.py /
COPY src/node/replicas/replica.py /

# Copiar la carpeta completa middleware
COPY src/middleware /middleware
COPY src/messages /messages
COPY src/utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]