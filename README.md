# Distribuidos-TP-Grupal
Trabajo Práctico grupal de Sistemas Distribuidos I FIUBA

[Informe](https://docs.google.com/document/d/1iqc8opaCAxscQxfFTXVvwTvbmNUf0Dy8diEn1TYtEW4/edit?usp=sharing)

### Ejecución del programa

Para correr el programa, ejecutar el siguiente comando en la terminal:

```bash
make docker-compose-up
```

### Detener el programa

Para terminar el programa, ejecutar el siguiente comando en la terminal:

```bash
make docker-compose-down
```

### Ver logs

Para ver los logs del programa, ejecutar el siguiente comando en la terminal:

```bash
make docker-compose-logs
```

### Escalabilidad: modificar cantidad de nodos

Para cambiar la cantidad de instancias de cada nodo que se levantarán en el sistema distribuido se debe editar el archivo [config.env](scripts\config.env) de la siguiente manera:

```
# Número de instancias de cada nodo
trimmer=1
genre=1
score=1
release_date=1
english=1

# Número de clientes
client=2

# Número de Replicas
propagator_replica=0
os_counter_replica=0
avg_counter_replica=0
q3_joiner_replica=0
q4_joiner_replica=0
q5_joiner_replica=3

# Número de Watchdogs
watchdog=1
```

Luego debe ejecutarse el siguiente script que modifica el archivo de Docker Compose con los datos de [config.env](scripts\config.env):

```bash
./scripts/generar-compose.sh
```

De todas maneras, al correr docker compose-up-logs eso ya corre el script para generar el compose.
El comando docker compose-up-logs muestra por pantalla directamente los logs luego de levantar el sistema.


### Datasets

Los distintos tipos de datasets se pueden descargar aqui: [Datasets](https://drive.google.com/drive/folders/1Oqcfio45qJbm07X3Ks3lup3A9c42F1HM?usp=drive_link)

Para seleccionar los datasets a utilizar, debe modificarse el config.ini del client.

Además los datasets deben colocarse en la carpeta ./datasets en la raiz del proyecto.

### Umbral Q4

En el config.ini del Q4joiner se puede modificar el umbral de la query 4. Por default es 5000 pero si se trabaja con datasets reducidos es conveniente modificarlo.

### Simulación de Fallas.

En el archivo [container_constants.py](src\utils\container_constants.py) se encuentran las distintas probabilidades utilizadas por el simulador de fallas.

```
ENDPOINTS_PROB_FAILURE = 0.00001 # últimos nodos del pipeline
REPLICAS_PROB_FAILURE = 0.00001 # réplicas
PROP_PROB_FAILURE = 0.0001 # propagador
FILTERS_PROB_FAILURE = 0.0001 # nodos filtro
```

Pueden modificarse esos valores para reducir o aumentar las caidas del sistema.