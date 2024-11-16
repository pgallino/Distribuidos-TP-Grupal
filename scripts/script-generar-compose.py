import os
import yaml
import sys

# Recibir argumentos del script de Bash que indican el número de instancias de cada nodo
def parse_args():
    try:
        # Los argumentos esperados son el número de instancias para cada nodo
        # Orden: trimmer, genre, score, release_date, english, os_counter, avg_counter
        args = sys.argv[1:7]
        instances = {
            'trimmer': int(args[0]),
            'genre': int(args[1]),
            'score': int(args[2]),
            'release_date': int(args[3]),
            'english': int(args[4]),
            'client': int(args[5]),        # client, joiners y counters tienen solo una instancia
            'q3_joiner': 1,
            'q4_joiner': 1,
            'q5_joiner': 1,
            'os_counter': 1,
            'avg_counter': 1,
            'os_counter_replica': 1  # Añadido como instancia fija
        }
        return instances
    except (IndexError, ValueError):
        print("Error: Asegúrate de pasar las instancias de los nodos en el orden correcto.")
        sys.exit(1)

# Generar el archivo YAML de Docker Compose basado en las instancias proporcionadas
def generate_docker_compose(instances):
    services = {}

    # Definición del servicio RabbitMQ
    services['rabbitmq'] = {
        'container_name': 'rabbitmq',
        'logging': {
            'driver': 'none'
        },
        'image': 'rabbitmq:3.13-management',
        'ports': [
            "5672:5672",
            "15672:15672"
        ],
        'environment': {
            'RABBITMQ_DEFAULT_USER': 'guest',
            'RABBITMQ_DEFAULT_PASS': 'guest'
        },
        'healthcheck': {
            'test': ["CMD", "rabbitmq-diagnostics", "status"],
            'interval': '10s',
            'timeout': '5s',
            'retries': 5,
            'start_period': '30s'
        },
        'networks': ['testing_net'],
        'privileged': True  # Añadir el modo privileged
    }

    # Definición del servidor principal
    services['server'] = {
        'container_name': 'server',
        'image': 'server:latest',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'LOGGING_LEVEL=INFO'
        ] + [
            f"{node.upper()}_INSTANCES={instances[node]}" for node in instances
        ],
        'depends_on': {
            'rabbitmq': {
                'condition': 'service_healthy'
            }
        },
        'networks': ['testing_net'],
        'privileged': True  # Añadir el modo privileged
    }

    # Generación de servicios con instancias, incluyendo nodos de una sola instancia
    for node, count in instances.items():
        for i in range(1, count + 1):
            service_name = f"{node}_{i}"
            services[service_name] = {
                'container_name': service_name,
                'image': f'{node}:latest',
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'LOGGING_LEVEL=INFO',
                    f'INSTANCE_ID={i}'
                ] + [
                    f"{other_node.upper()}_INSTANCES={instances[other_node]}" for other_node in instances
                ],
                'depends_on': {
                    'rabbitmq': {
                        'condition': 'service_healthy'
                    }
                },
                'networks': ['testing_net'],
                'privileged': True  # Añadir el modo privileged
            }

            # Si es el cliente, añadir dependencia de servidor y volumen datasets
            if node == 'client':
                services[service_name]['depends_on']['server'] = {
                    'condition': 'service_started'
                }
                services[service_name]['volumes'] = [f'./datasets:/datasets', f'./results:/results']
                
    # Definición de la estructura completa de Docker Compose
    docker_compose_dict = {
        'name': 'steamyanalytics',
        'services': services,
        'networks': {
            'testing_net': {
                'ipam': {
                    'driver': 'default',
                    'config': [
                        {'subnet': '172.25.125.0/24'}
                    ]
                }
            }
        }
    }

    return docker_compose_dict

# Guardar el diccionario de Docker Compose en un archivo YAML
def save_docker_compose_file(docker_compose_dict, file_name='docker-compose-dev.yaml'):

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(project_root, file_name)

    with open(file_path, 'w') as file:
        yaml.dump(docker_compose_dict, file, default_flow_style=False)
    print(f'Archivo {file_path} generado con éxito.')

def main():
    # Obtener instancias desde los argumentos pasados desde el script de Bash
    instances = parse_args()

    # Generar diccionario de Docker Compose
    docker_compose_dict = generate_docker_compose(instances)

    # Guardar el archivo docker-compose-dev.yaml
    save_docker_compose_file(docker_compose_dict)

if __name__ == '__main__':
    main()
