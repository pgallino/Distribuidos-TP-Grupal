FROM python:3.9.7-slim

# Copiamos los archivos necesarios
COPY client/main.py /
COPY client/config.ini /
COPY client/common /common

COPY messages /messages

COPY utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]