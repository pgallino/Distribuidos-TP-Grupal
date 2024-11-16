FROM python:3.9.7-slim

# Copiamos los archivos necesarios
COPY src/client/main.py /
COPY src/client/config.ini /
COPY src/client/client.py /

COPY src/messages /messages

COPY src/utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]