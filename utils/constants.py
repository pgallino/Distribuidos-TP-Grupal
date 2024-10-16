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
Q_COORD_GENRE = 'coord-genre'
Q_GATEWAY_TRIMMER = 'gateway-trimmer'
Q_QUERY_RESULT_1 = "query_result_1"
Q_QUERY_RESULT_2 = "query_result_2"
Q_QUERY_RESULT_3 = "query_result_3"
Q_QUERY_RESULT_4 = "query_result_4"
Q_QUERY_RESULT_5 = "query_result_5"
Q_GENRE_Q5_JOINER = "genre-q5-joiner"
Q_SCORE_Q5_JOINER = "score-q5-joiner"
Q_ENGLISH_Q4_JOINER = 'english-q4_joiner'
Q_GENRE_Q4_JOINER = 'genre-joiner_q4'
Q_GENRE_Q3_JOINER = "genre-q3-joiner"
Q_SCORE_Q3_JOINER = "score-q3-joiner"
Q_TRIMMER_SCORE_FILTER = "trimmer-score_filter"
Q_SCORE_ENGLISH = "score_filter-english_filter"
Q_COORD_SCORE = 'coord-score'
Q_GENRE_RELEASE_DATE = "genre-release_date"
Q_COORD_TRIMMER = 'coord-trimmer'
Q_COORD_ENGLISH = 'coord-english'
Q_TRIMMER_OS_COUNTER = "trimmer-os_counter"
Q_RELEASE_DATE_AVG_COUNTER = 'release_date-avg_counter'
Q_COORD_RELEASE_DATE = 'coord-release_date'
Q_SCORE_Q4_JOINER = 'score-q4_joiner'
Q_Q4_JOINER_ENGLISH = 'q4_joiner-english'

# Exchange Names
E_FROM_TRIMMER = 'trimmer-filters'
E_TRIMMER_FILTERS = 'trimmer-filters'
E_FROM_GENRE = 'from_genre'
E_COORD_GENRE = 'from-coord-genre'
E_COORD_TRIMMER = 'from-coord-trimmer'
E_FROM_SCORE = "from_score"
E_COORD_SCORE = 'from-coord-score'
E_COORD_RELEASE_DATE = 'from-coord-release_date'
E_COORD_ENGLISH = 'from-coord-english'

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
