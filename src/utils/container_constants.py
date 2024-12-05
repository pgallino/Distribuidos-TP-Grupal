LISTENER_PORT = 12345
ELECTION_PORT = 8080

SERVER_CONTAINER_NAME = "server"
TRIMMER_CONTAINER_NAME = "trimmer"
GENRE_CONTAINER_NAME = "genre"
SCORE_CONTAINER_NAME = "score"
RELEASE_DATE_CONTAINER_NAME = "release_date"
ENGLISH_CONTAINER_NAME = "english"
OS_COUNTER_CONTAINER_NAME = "os_counter"
AVG_COUNTER_CONTAINER_NAME = "avg_counter"
Q3_JOINER_CONTAINER_NAME = "q3_joiner"
Q4_JOINER_CONTAINER_NAME = "q4_joiner"
Q5_JOINER_CONTAINER_NAME = "q5_joiner"
OS_COUNTER_REPLICA_CONTAINER_NAME = "os_counter_replica"
AVG_COUNTER_REPLICA_CONTAINER_NAME = "avg_counter_replica"
Q3_JOINER_REPLICA_CONTAINER_NAME = "q3_joiner_replica"
Q4_JOINER_REPLICA_CONTAINER_NAME = "q4_joiner_replica"
Q5_JOINER_REPLICA_CONTAINER_NAME = "q5_joiner_replica"
WATCHDOG_CONTAINER_NAME = "watchdog"
PROPAGATOR_REPLICA = "propagator_replica"
PROPAGATOR = "propagator"

GENERAL_CONFIG_KEYS = ["instance_id", "logging_level"]

SERVER_CONFIG_KEYS = [
    "server_port",
    "server_listen_backlog",
    f'trimmer_instances',
    "logging_level"
]

WATCHDOG_NODES_TO_MONITOR = [
    TRIMMER_CONTAINER_NAME,
    GENRE_CONTAINER_NAME,
    SCORE_CONTAINER_NAME,
    RELEASE_DATE_CONTAINER_NAME,
    ENGLISH_CONTAINER_NAME,
    OS_COUNTER_CONTAINER_NAME,
    AVG_COUNTER_CONTAINER_NAME,
    Q3_JOINER_CONTAINER_NAME,
    Q4_JOINER_CONTAINER_NAME,
    Q5_JOINER_CONTAINER_NAME,
    OS_COUNTER_REPLICA_CONTAINER_NAME,
    AVG_COUNTER_REPLICA_CONTAINER_NAME,
    Q3_JOINER_REPLICA_CONTAINER_NAME,
    Q4_JOINER_REPLICA_CONTAINER_NAME,
    Q5_JOINER_REPLICA_CONTAINER_NAME,
    PROPAGATOR_REPLICA,
    PROPAGATOR
]

# Generar WATCHDOG_CONFIG_KEYS dinámicamente
WATCHDOG_CONFIG_KEYS = [f'watchdog_instances'] + [
    f"{node}_instances" for node in WATCHDOG_NODES_TO_MONITOR
] + GENERAL_CONFIG_KEYS

OS_COUNTER_REPLICA_CONFIG_KEYS = [
    "os_counter_replica_instances",
    "timeout"
] + GENERAL_CONFIG_KEYS

AVG_COUNTER_REPLICA_CONFIG_KEYS = [
    "avg_counter_replica_instances",
    "timeout"
] + GENERAL_CONFIG_KEYS

Q3_JOINER_REPLICA_CONFIG_KEYS = [
    "q3_joiner_replica_instances",
    "timeout"
] + GENERAL_CONFIG_KEYS

Q4_JOINER_REPLICA_CONFIG_KEYS = [
    "q4_joiner_replica_instances",
    "timeout",
] + GENERAL_CONFIG_KEYS

Q5_JOINER_REPLICA_CONFIG_KEYS = [
    "q5_joiner_replica_instances",
    "timeout",
] + GENERAL_CONFIG_KEYS

TRIMMER_NEXT_NODES = [
    GENRE_CONTAINER_NAME,
    SCORE_CONTAINER_NAME,
    OS_COUNTER_CONTAINER_NAME
]

TRIMMER_CONFIG_KEYS = [
    "trimmer_instances"
] + [f"{node}_instances" for node in TRIMMER_NEXT_NODES] + GENERAL_CONFIG_KEYS

Q5_JOINER_CONFIG_KEYS = [
    "q5_joiner_instances",
    "q5_joiner_replica_instances"
] + GENERAL_CONFIG_KEYS

Q4_JOINER_CONFIG_KEYS = [
    "q4_joiner_instances",
    "n_reviews",
    "max_batch_size",
    "english_instances",
    "q4_joiner_replica_instances"
] + GENERAL_CONFIG_KEYS

Q3_JOINER_CONFIG_KEYS = [
    "q3_joiner_instances",
    "q3_joiner_replica_instances"
] + GENERAL_CONFIG_KEYS

SCORE_FILTER_NEXT_NODES = [
    Q3_JOINER_CONTAINER_NAME,
    Q4_JOINER_CONTAINER_NAME,
    Q5_JOINER_CONTAINER_NAME
]

SCORE_FILTER_CONFIG_KEYS = [
    "score_instances"
] + [f"{node}_instances" for node in SCORE_FILTER_NEXT_NODES] + GENERAL_CONFIG_KEYS

RELEASE_DATE_FILTER_NEXT_NODES = [
    AVG_COUNTER_CONTAINER_NAME
]

RELEASE_DATE_FILTER_CONFIG_KEYS = [
    "release_date_instances"
] + [f"{node}_instances" for node in RELEASE_DATE_FILTER_NEXT_NODES] + GENERAL_CONFIG_KEYS

GENRE_FILTER_NEXT_NODES = [
    RELEASE_DATE_CONTAINER_NAME,
    Q3_JOINER_CONTAINER_NAME,
    Q4_JOINER_CONTAINER_NAME,
    Q5_JOINER_CONTAINER_NAME
]

GENRE_FILTER_CONFIG_KEYS = [
    "genre_instances"
] + [f"{node}_instances" for node in GENRE_FILTER_NEXT_NODES] + GENERAL_CONFIG_KEYS

ENGLISH_FILTER_NEXT_NODES = [
    Q4_JOINER_CONTAINER_NAME
]

ENGLISH_FILTER_CONFIG_KEYS = [
    "english_instances"
] + [f"{node}_instances" for node in ENGLISH_FILTER_NEXT_NODES] + GENERAL_CONFIG_KEYS

OS_COUNTER_CONFIG_KEYS = [
    "os_counter_instances",
    "os_counter_replica_instances"
] + GENERAL_CONFIG_KEYS

AVG_COUNTER_CONFIG_KEYS = [
    "avg_counter_instances",
    "avg_counter_replica_instances"
] + GENERAL_CONFIG_KEYS

CLIENT_CONFIG_KEYS = [
    "server_ip",
    "server_port",
    "games_dataset",
    "reviews_dataset",
    "max_batch_size"
] + GENERAL_CONFIG_KEYS


ENDPOINTS_PROB_FAILURE = 0.00001
REPLICAS_PROB_FAILURE = 0.00001
FILTERS_PROB_FAILURE = 0
