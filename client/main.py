import socket

def main():
    ip = "server"
    port = 12345
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client_socket.connect((ip, port))
        print("Connected to the server")

        message = "Hello Server"
        client_socket.send(message.encode("utf-8"))
        client_socket.send(message.encode("utf-8")) 
        client_socket.send(message.encode("utf-8")) 

        message = "FIN"
        client_socket.send(message.encode("utf-8")) 

    except Exception as error:
        print(f"Error: {error}")
    
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()