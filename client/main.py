from common.client import Client
import utils.logging_config # Esto ejecuta la configuración del logger

def main():

    ip = "server"
    port = 12345

    client = Client(0, (ip, port))
    client.run()


if __name__ == "__main__":
    main()