FROM python:3.9.7-slim

# Instalar las librerías necesarias directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0 langid==1.1.6

# Copiamos los archivos necesarios
COPY src/nodes/filters/english/main.py /
COPY src/nodes/filters/english/english_filter.py /

COPY src/nodes/node.py /

# Copiar la carpeta completa middleware
COPY src/middleware /middleware
COPY src/messages /messages
COPY src/utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]