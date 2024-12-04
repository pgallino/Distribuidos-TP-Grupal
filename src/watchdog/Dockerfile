FROM python:3.9.7-slim

# Instalar Docker CLI y solo las dependencias esenciales
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copiamos los archivos necesarios
COPY src/watchdog/main.py /
COPY src/watchdog/watchdog.py /
COPY src/listener /listener

COPY src/messages /messages
COPY src/utils /utils
COPY src/election /election

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]