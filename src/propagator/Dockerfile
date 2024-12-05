FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY src/propagator/main.py /
COPY src/propagator/propagator.py /
COPY src/listener/listener.py /

COPY src/messages /messages
COPY src/utils /utils
COPY src/middleware /middleware
# COPY src/election /election

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]