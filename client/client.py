import socket

class Client:
    def __init__(self, host='127.0.0.1', port=5000):
        self.host = host
        self.port = port

    def send_request(self, command):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((self.host, self.port))
                client_socket.sendall(command.encode('utf-8'))
                response = client_socket.recv(1024).decode('utf-8')
                return response
        except Exception as e:
            return f"Error connecting to {self.host}:{self.port} - {e}"

    def put(self, key, value):
        try:
            return self.send_request(f"PUT {key} {value}")
        except Exception as e:
            return f"Error during PUT operation: {e}"

    def get(self, key):
        try:
            return self.send_request(f"GET {key}")
        except Exception as e:
            return f"Error during GET operation: {e}"
        

# Primary server client
primary_client = Client(host="127.0.0.1", port=5000)

# Replica servers clients
replica_clients = [
    Client(host="127.0.0.1", port=5001),  # Replica 1
    Client(host="127.0.0.1", port=5002),  # Replica 2
]

# PUT operation on the primary server
key = "key1"
# value = "value1"
# print(f"Primary PUT Response: {primary_client.put(key, value)}")

# client = Client(host="127.0.0.1", port=5000)
# replica_client = Client(host="127.0.0.1", port=5001)
print(primary_client.get("key1"))
# print(client.put("key1", "value1"))

# replica_client = Client(host="127.0.0.1", port=5001)
# print(replica_client.get("key1"))
