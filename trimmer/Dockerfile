FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0

# Copiamos los archivos necesarios
COPY trimmer/main.py /
COPY trimmer/common /common

# Copiar la carpeta completa middleware
COPY middleware /middleware
COPY messages /messages
COPY utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]