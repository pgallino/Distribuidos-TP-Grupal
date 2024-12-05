FROM python:3.9.7-slim


# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY src/replicas/avg_counter_replica/main.py /
COPY src/replicas/avg_counter_replica/avg_counter_replica.py /
COPY src/replicas/replica.py /
COPY src/listener/listener.py /
COPY src/replicas/config.ini /

COPY src/middleware /middleware
COPY src/messages /messages
COPY src/utils /utils
COPY src/election /election

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]