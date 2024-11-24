import socket
import threading
from utils.data_structures import InMemoryStorage, PersistentStorage
from utils.hashing import ConsistentHashing  # New utility for consistent hashing
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/server.log"),  # Logs to file
        logging.StreamHandler()                 # Logs to console
    ]
)

# Set up a thread pool with a limit on the number of threads
thread_pool = ThreadPoolExecutor(max_workers=5)

class Server:
    def __init__(self, host='127.0.0.1', port=5000, replicas=None, node_id=None):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.storage = PersistentStorage(storage_file=f"server_{port}_storage.json")
        # Remove self from replicas list
        if replicas:
            self.replicas = [
                replica for replica in replicas if replica != (self.host, self.port)
            ]
        else:
            self.replicas = []
        # self.replicas = self.validate_replicas(replicas)
        logging.info(f"Validated replicas: {self.replicas}")
        self.consistent_hashing = ConsistentHashing(self.replicas)  # Add self to the hash ring

    def handle_client(self, conn, addr):
        try:
            logging.info(f"Connection established with {addr}")
            while True:
                try:
                    data = conn.recv(1024).decode('utf-8').strip()
                    logging.info(f"Raw received data: {data}")
                    if not data:
                        break  # Close the connection if no data is received
                    logging.info(f"Received data: {data} from {addr}")
                    # Handle HEARTBEAT
                    if data == "HEARTBEAT":
                        conn.sendall("ALIVE".encode('utf-8'))
                        continue
                    
                    # Handle PUT and GET commands
                    if data.startswith("PUT"):
                        # Split the input and check length
                        command_parts = data.split(None)
                        if len(command_parts) < 3:
                            logging.error(f"Malformed PUT command: {data} from {addr}")
                            response = "Error: PUT command must be in the format 'PUT <key> <value>'."
                        else:
                            _, key, value = command_parts[:3]
                            # Check if the request is a replication request
                            is_replication = "replication=true" in command_parts
                            response = self.handle_put(key, value, is_replication)

                    elif data.startswith("GET"):
                        # Split the input and check length
                        command_parts = data.split(None)
                        if len(command_parts) != 2:
                            logging.error(f"Malformed GET command: {data} from {addr}")
                            response = "Error: GET command must be in the format 'GET <key>'."
                        else:
                            _, key = command_parts
                            response = self.handle_get(key)
                    else:
                        response = "Invalid command. Use PUT <key> <value> or GET <key>."
                    
                    conn.sendall(response.encode('utf-8'))
                except ConnectionResetError:
                    break
                except Exception as e:
                    logging.error(f"Error processing request from {addr}: {e}")
                    conn.sendall(f"Error: {e}".encode('utf-8'))
        except Exception as e:
            logging.error(f"Error handling client {addr}: {e}")
        finally:
            conn.close()

    def handle_put(self, key, value, is_replication=False):
        response = self.storage.put(key, value)
        # Determine responsible replicas using consistent hashing
        if not is_replication:
            responsible_replicas = self.consistent_hashing.get_replicas(key)
            logging.info(f"Responsible replicas for key '{key}': {responsible_replicas}")
            if not responsible_replicas:
                logging.error(f"No responsible replicas found for key '{key}'.")
                return "Error: No responsible replicas found."
            for replica in responsible_replicas:
                self.replicate_put(replica, key, value)
        return response

    def handle_get(self, key):
        return self.storage.get(key)

    def replicate_put(self, replica, key, value):
        def replicate_task(replica, key, value):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
                    replica_socket.connect(replica)
                    replica_socket.sendall(f"PUT {key} {value} replication=true".encode('utf-8'))
                    logging.info(f"Replication success for key '{key}' to replica {replica}")
            except Exception as e:
                logging.error(f"Error replicating key '{key}' to replica {replica}: {e}")
        threading.Thread(target=replicate_task, args=(replica, key, value)).start()

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            print(f"Server started on {self.host}:{self.port}")
            logging.info(f"Server started on {self.host}:{self.port}")  # Log confirmation
            while True:
                conn, addr = server_socket.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                thread.start()
    
    def recover_data(self, node):
        try:
            logging.info(f"Attempting to recover data for node {node}")
            # Assume we can query replica nodes for missing data
            for key in self.storage.keys():
                responsible_node = self.consistent_hashing.get_node(key)
                if responsible_node == (self.host, self.port):  # Current node is responsible
                    # Re-fetch missing data
                    self.storage.put(key, self.fetch_data_from_replicas(key))
        except Exception as e:
            logging.error(f"Error recovering data for node {node}: {e}")
    
    def validate_replicas(self, replicas):
        validated_replicas = []
        for replica in replicas:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(2)  # Timeout for connection attempt
                    sock.connect(replica)
                validated_replicas.append(replica)
            except (socket.error, ConnectionRefusedError):
                logging.warning(f"Replica {replica} is unreachable and will be excluded.")
        return validated_replicas


    def fetch_data_from_replicas(self, key):
        for replica in self.replicas:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(replica)
                    sock.sendall(f"GET {key}".encode('utf-8'))
                    response = sock.recv(1024).decode('utf-8')
                    if not response.startswith("Error"):
                        return response
            except Exception as e:
                logging.error(f"Failed to fetch key '{key}' from replica {replica}: {e}")
        return None


# Parse command-line arguments for port and optional replicas
parser = argparse.ArgumentParser(description="Start a distributed key-value store server.")
parser.add_argument("--host", type=str, default="127.0.0.1", help="Host IP address of the server (default: 127.0.0.1)")
parser.add_argument("--port", type=int, required=True, help="Port number the server will bind to.")
parser.add_argument(
    "--replicas",
    type=str,
    nargs="*",
    help="List of replica nodes in the format 'host:port', separated by spaces.",
)
parser.add_argument("--node-id", type=str, default=None, help="Unique identifier for the server node (optional).")
args = parser.parse_args()

# Parse replicas into tuples of (host, port)
replicas = []
if args.replicas:
    for replica in args.replicas:
        try:
            host, port = replica.split(":")
            replicas.append((host, int(port)))
        except ValueError:
            print(f"Invalid replica format: {replica}. Use 'host:port'.")
            exit(0)

# Create and start the server
server = Server(host=args.host, port=args.port, replicas=replicas, node_id=args.node_id)
print(f"Starting server on {args.host}:{args.port} with replicas: {replicas}")
server.start_server()