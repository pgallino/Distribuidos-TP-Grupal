FROM python:3.9.7-slim

# Instalar la librería pika directamente usando pip
RUN pip install --no-cache-dir pika==1.2.0
RUN pip install --no-cache-dir numpy


# Copiamos los archivos necesarios
COPY node/joiners/q5_joiner/main.py /
COPY node/joiners/q5_joiner/q5_joiner.py /
COPY node/node.py /
COPY node/coordinator.py /

# Copiar la carpeta completa middleware
COPY middleware /middleware
COPY messages /messages
COPY utils /utils

# Configura el ENTRYPOINT para ejecutar main.py al iniciar
ENTRYPOINT ["python3", "/main.py"]