import socket
import threading
from utils.data_structures import InMemoryStorage, PersistentStorage
from utils.hashing import ConsistentHashing  # New utility for consistent hashing
import logging
import argparse
from server.health_monitor import MerkleTree
from concurrent.futures import ThreadPoolExecutor
from utils.backup import BackupManager
import os

# Ensure logs directory exists
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/server.log"),  # Logs to file
        logging.StreamHandler()                 # Logs to console
    ]
)

# Example log
logging.info("Logging has been successfully configured.")

hashing_list = None

# Set up a thread pool with a limit on the number of threads
thread_pool = ThreadPoolExecutor(max_workers=5)
class TransactionManager:
    """
    Manages transaction states and ensures atomicity, consistency, isolation, and durability (ACID).
    """
    def __init__(self):
        self.transactions = {}  # {transaction_id: {"state": "PREPARED/COMMITTED/ABORTED", "data": {key: value}}}
        logging.basicConfig(level=logging.INFO)

    def prepare(self, transaction_id, operations):
        if transaction_id in self.transactions:
            logging.error(f"Transaction {transaction_id} already exists. Cannot prepare again.")
            return False
        self.transactions[transaction_id] = {"state": "PREPARED", "data": operations}
        logging.info(f"Transaction {transaction_id} prepared with operations: {operations}")
        return True

    def commit(self, transaction_id, storage):
        if transaction_id not in self.transactions:
            logging.error(f"Transaction {transaction_id} not found. Cannot commit.")
            return False
        if self.transactions[transaction_id]["state"] == "PREPARED":
            # Apply the changes from the transaction data to storage
            for key, value in self.transactions[transaction_id]["data"].items():
                storage[key] = value
            self.transactions[transaction_id]["state"] = "COMMITTED"
            logging.info(f"Transaction {transaction_id} committed.")
            return True
        logging.error(f"Transaction {transaction_id} is not in PREPARED state, cannot commit.")
        return False

    def rollback(self, transaction_id):
        if transaction_id not in self.transactions:
            logging.error(f"Transaction {transaction_id} not found. Cannot rollback.")
            return False
        if self.transactions[transaction_id]["state"] == "PREPARED":
            del self.transactions[transaction_id]
            logging.info(f"Transaction {transaction_id} rolled back.")
            return True
        logging.error(f"Transaction {transaction_id} is not in PREPARED state, cannot rollback.")
        return False

class Server:
    def __init__(self, host='127.0.0.1', port=5000, replicas=None, node_id=None, backup_interval=300):
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
        logging.info(f"Validated replicas: {self.replicas}")
        self.consistent_hashing = ConsistentHashing(self.replicas)  # Add self to the hash ring
        hashing_list = self.consistent_hashing
        self.merkle_tree = MerkleTree()
        # Initialize BackupManager with a periodic backup interval of 5 minutes (300 seconds)
        self.backup_manager = BackupManager(self.storage)
    
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

                    # Handle TRANSACTION commands (PREPARE, COMMIT, ROLLBACK)
                    if data.startswith("TRANSACTION"):
                        # Split the input and check the format
                        command_parts = data.split(None)
                        if len(command_parts) < 3:
                            logging.error(f"Malformed TRANSACTION command: {data} from {addr}")
                            response = "Error: TRANSACTION command must be in the format 'TRANSACTION <id> PREPARE|COMMIT|ROLLBACK'."
                        else:
                            transaction_id = command_parts[1]
                            if command_parts[2] == "PREPARE":
                                response = self.handle_prepare(transaction_id, command_parts[3:])
                            elif command_parts[2] == "COMMIT":
                                response = self.handle_commit(transaction_id)
                            elif command_parts[2] == "ROLLBACK":
                                response = self.handle_rollback(transaction_id)
                            else:
                                response = "Invalid TRANSACTION command. Use PREPARE, COMMIT, or ROLLBACK."
                    # Handle PUT and GET commands
                    elif data.startswith("PUT"):
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
        self.storage[key] = value  # This will invoke __setitem__ in PersistentStorage
        self.storage.save_data()  # Ensure the data is saved after the operation
        
        response = f"PUT {key}={value} OK"
        
        self.merkle_tree.add_leaf(key, value)
        root_hash = self.merkle_tree.build_tree()
        # Log the updated root hash
        logging.info(f"Updated Merkle Tree Root Hash: {root_hash}")
        # Log the PUT operation for replication if necessary
        if is_replication:
            self.backup_manager.log_write(f"PUT {key} {value}")
        #     if not is_replication:
        # self.backup_manager.log_write(f"PUT {key} {value}")
        # Replicate the PUT operation to the replicas
            for replica in self.replicas:
                self.replicate_put(replica, key, value, root_hash)
        return response

    def handle_get(self, key):
        value = self.storage[key]
        # This will invoke __getitem__ in PersistentStorage
        if value is None:
            return f"Error: Key '{key}' not found."
        return f"GET {key}={value}"

    def integrity_check(self):
        while True:
            root_hash = self.merkle_tree.build_tree()
            for node in self.active_nodes:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect(node)
                        sock.sendall(f"CHECK_HASH {root_hash}".encode('utf-8'))
                        response = sock.recv(1024).decode('utf-8')
                        if response != "MATCH":
                            logging.warning(f"Hash mismatch with node {node}")
                            self.resync_with_node(node)
                except Exception as e:
                    logging.error(f"Error checking integrity with node {node}: {e}")
            time.sleep(30)  # Check every 30 seconds

    def replicate_put(self, replica, key, value, root_hash):
        # Define the replicate task that will be executed by the thread
        def replicate_task(replica, key, value):
            try:
                # Create a socket to connect to the replica
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as replica_socket:
                    # Connect to the replica server
                    replica_socket.connect(replica)
                    
                    # Send PUT command with root hash for verification
                    command = f"PUT {key} {value} ROOT_HASH {root_hash}"
                    replica_socket.sendall(command.encode('utf-8'))
                    
                    # Format the PUT request with the replication flag
                    # command = f"PUT {key} {value} replication=true"
                    # replica_socket.sendall(command.encode('utf-8'))  # Send the data to the replica
                    
                    logging.info(f"Replication success for key '{key}' to replica {replica}")
            except Exception as e:
                # Handle any errors that occur during replication
                logging.error(f"Error replicating key '{key}' to replica {replica}: {e}")
        
        # Create and start the thread for non-blocking replication
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
# Example: Save a snapshot every 10 minutes
import time

# while True:
#     time.sleep(6)  # Sleep for 10 minutes
#     self.backup_manager.save_snapshot()
