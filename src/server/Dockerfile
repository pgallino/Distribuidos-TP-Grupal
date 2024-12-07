FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY src/server/main.py /
COPY src/server/server.py /
COPY src/server/connection_handler.py /
COPY src/server/result_dispatcher.py /
COPY src/server/config.ini /

COPY src/messages /messages

COPY src/middleware /middleware

COPY src/utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]
