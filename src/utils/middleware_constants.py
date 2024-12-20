# constants.py

#### ESTANDAR NOMBRE COLAS ####
# Q_ORIGEN_DESTINO = "origen-destino"
# Q_COORD_NODO = "coord-nodo"

#### ESTANDAR NOMBRE EXCHANGES ####
# E_FROM_ORIGEN = "from_origen"

#### ESTANDAR NOMBRE CLAVES ####
# K_GAME = "game"

# Queue Names
Q_TRIMMER_GENRE_FILTER = "trimmer-genre_filter"
Q_GATEWAY_TRIMMER = 'gateway-trimmer'
Q_QUERY_RESULT = 'query_result'
Q_QUERY_RESULT_1 = "query_result_1"
Q_QUERY_RESULT_2 = "query_result_2"
Q_QUERY_RESULT_3 = "query_result_3"
Q_QUERY_RESULT_4 = "query_result_4"
Q_QUERY_RESULT_5 = "query_result_5"
Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_GENRE_Q4_JOINER = 'genre-q4-joiner'
Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
Q_SCORE_ENGLISH = "score_filter-english_filter"
Q_GENRE_RELEASE_DATE = "genre-release_date"
Q_TRIMMER_OS_COUNTER = "trimmer-os_counter"
Q_RELEASE_DATE_AVG_COUNTER = 'release_date-avg_counter'
Q_SCORE_Q4_JOINER = 'score-q4_joiner'
Q_Q4_JOINER_ENGLISH = 'q4_joiner-english'
Q_REPLICA_MASTER = 'replica_master'
Q_MASTER_REPLICA = 'master_replica'
Q_TO_PROP = 'propagation'
Q_NOTIFICATION = 'notification'
Q_REPLICA_SYNC_REQUEST_LISTENER = 'replica-sync-request-listener'
Q_Q3_JOINER = 'q3-joiner'
Q_Q5_JOINER = 'q5-joiner'

# Exchange Names
E_FROM_TRIMMER = 'trimmer-filters'
E_FROM_GENRE = 'from_genre'
E_FROM_SCORE = "from_score"
E_FROM_MASTER_PUSH = 'from-master-push'
E_FROM_REPLICA_PULL_ANS = 'from-replica-pull-ans'
E_FROM_PROP = 'from-propagator'
E_REPLICA_SYNC_REQUEST_LISTENER = 'from-replica-sync-listener'
E_SYNC_STATE = 'sync'

# Routing Keys
K_GENREGAME = 'genregame'
K_INDIE_Q2GAMES = 'indie_q2games'
K_INDIE_BASICGAMES = 'indie_basicgames'
K_SHOOTER_GAMES = 'shooter_games'
K_NEGATIVE = 'negative'
K_Q1GAME = 'q1game'
K_POSITIVE = 'positive'
K_REVIEW = 'review'
K_NEGATIVE_TEXT = 'negative_text'
K_NOTIFICATION = 'notification'
K_FIN = 'fin'
