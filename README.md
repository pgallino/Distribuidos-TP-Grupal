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

Para cambiar la cantidad de instancias de cada nodo que se levantarán en el sistema distribuido se puede usar el siguiente script que modifica el archivo de Docker Compose:

```bash
./generar-compose.sh trimmer=2 genre=3 score=1 release_date=4 english=5 os_counter=2 average_counter=1
```