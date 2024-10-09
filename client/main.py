from common.client import Client

def main():

    ip = "server"
    port = 12345

    client = Client(0, (ip, port))
    client.run()


if __name__ == "__main__":
    main()