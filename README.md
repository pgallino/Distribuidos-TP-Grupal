# Distribuidos-TP-Grupal
Trabajo Práctico grupal de Sistemas Distribuidos I FIUBA

[Informe](https://docs.google.com/document/d/1iqc8opaCAxscQxfFTXVvwTvbmNUf0Dy8diEn1TYtEW4/edit?usp=sharing)

[Diagramas](https://drive.google.com/file/d/1Cm5oy1AQicfzJ9OTNVRzGIoBI0zwK73y/view?usp=sharing)

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
os_counter_replica=1
avg_counter_replica=1
```

Luego debe ejecutarse el siguiente script que modifica el archivo de Docker Compose con los datos de [config.env](scripts\config.env):

```bash
./scripts/generar-compose.sh
```


### Datasets

Los distintos tipos de datasets se pueden descargar aqui: [Datasets](https://drive.google.com/drive/folders/1Oqcfio45qJbm07X3Ks3lup3A9c42F1HM?usp=drive_link)