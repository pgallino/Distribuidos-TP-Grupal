FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY server/main.py /
COPY server/server.py /
COPY server/connection_handler.py /
COPY server/result_dispatcher.py /

COPY messages /messages

COPY middleware /middleware

COPY utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]
