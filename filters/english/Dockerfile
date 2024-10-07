FROM python:3.9.7-slim

# Instalar las librerías necesarias directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0 langid==1.1.6

# Copiamos los archivos necesarios
COPY filters/english/main.py /
COPY filters/english/common /common

# Copiar la carpeta completa middleware
COPY middleware /middleware
COPY messages /messages
COPY utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]