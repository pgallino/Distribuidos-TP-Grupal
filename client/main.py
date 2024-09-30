import socket

def main():
    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client_socket.connect((ip, port))
        print("Connected to the server")

        message = "Hello Server"
        for i in range(1):
            curr_message = message + f" {i}\n"
            client_socket.send(curr_message.encode("utf-8"))

        # Envía el mensaje "FIN" con un delimitador
        message = "FIN\n"
        client_socket.send(message.encode("utf-8"))

    except Exception as error:
        print(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()