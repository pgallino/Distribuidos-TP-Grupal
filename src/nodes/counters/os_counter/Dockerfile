FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY src/nodes/counters/os_counter/main.py /
COPY src/nodes/counters/os_counter/os_counter.py /
COPY src/nodes/node.py /
COPY src/listener/listener.py /


# Copiar la carpeta completa middleware
COPY src/middleware /middleware
COPY src/messages /messages
COPY src/utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]