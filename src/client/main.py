from client import Client
from utils.initilization import initialize_config, initialize_log

def main():

    required_keys = {
        "instance_id": ("INSTANCE_ID", "INSTANCE_ID"),
        "ip": ("SERVER_IP", "SERVER_IP"),
        "port": ("SERVER_PORT", "SERVER_PORT"),
        "games": ("GAMES_DATASET", "GAMES_DATASET"),
        "reviews": ("REVIEWS_DATASET", "REVIEWS_DATASET"),
        "batch": ("MAX_BATCH_SIZE", "MAX_BATCH_SIZE"),
        "logging_level": ("LOGGING_LEVEL", "LOGGING_LEVEL")
    }

    config_params = initialize_config(required_keys)
    id = config_params["instance_id"]
    ip = config_params["ip"]
    port = config_params["port"]
    batch_size = config_params["batch"]
    games = config_params["games"]
    reviews = config_params["reviews"]
    logging_level = config_params["logging_level"]

    initialize_log(logging_level)

    # client = Client(int(os.environ['INSTANCE_ID']), (ip, port), batch_size)
    client = Client(id, (ip, port), batch_size, games, reviews)
    client.run()
    # if id == 1:
    #     client.run()


if __name__ == "__main__":
    main()