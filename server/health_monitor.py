import threading
import time
import socket
import logging
import hashlib
# from .server import hashing_list

class HealthMonitor:
    def __init__(self, nodes, heartbeat_interval=5):
        self.nodes = nodes  # List of other nodes
        self.heartbeat_interval = heartbeat_interval
        self.active_nodes = set(nodes)

    def send_heartbeat(self, node):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect(node)
                sock.sendall("HEARTBEAT".encode('utf-8'))
                response = sock.recv(1024).decode('utf-8')
                if response != "ALIVE":
                    raise Exception("Unexpected response")
            return True
        except:
            return False
    
    # def redistribute_data(self, failed_node):
    #     logging.warning(f"Redistributing data from failed node {failed_node}")
        
    #     # Remove the failed node from consistent hashing
    #     # hashing_list.remove_node(failed_node)
        
    #     # Retrieve keys for which the failed node was responsible
    #     # failed_keys = hashing_list.get_keys_responsible(failed_node)  # Add logic to get keys for failed node
    #     for key in failed_keys:
    #         # Use replicas to retrieve the value
    #         value = self.retrieve_from_replicas(key)  # Ensure replication logic exists
    #         responsible_node = hashing_list.get_node(key)
    #         try:
    #             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #                 sock.connect(responsible_node)
    #                 sock.sendall(f"PUT {key} {value}".encode('utf-8'))
    #             logging.info(f"Key '{key}' redistributed to node {responsible_node}")
    #         except Exception as e:
    #             logging.error(f"Failed to redistribute key '{key}' to node {responsible_node}: {e}")
    
    def retrieve_from_replicas(self, key):
        # Add logic to fetch the key's value from available replicas
        for node in self.active_nodes:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(node)
                    sock.sendall(f"GET {key}".encode('utf-8'))
                    response = sock.recv(1024).decode('utf-8')
                    if "Error" not in response:
                        return response  # Key value retrieved
            except:
                continue
        logging.error(f"Key '{key}' not found in any replica.")
        return None

    def monitor_nodes(self):
        while True:
            for node in self.nodes:
                if not self.send_heartbeat(node):
                    logging.error(f"Node {node} is DOWN.")
                    self.active_nodes.discard(node)
                    self.redistribute_data(node)
                else:
                    self.active_nodes.add(node)
            time.sleep(self.heartbeat_interval)

    def start_monitoring(self):
        threading.Thread(target=self.monitor_nodes, daemon=True).start()
class MerkleTree:
    def __init__(self):
        self.leaves = []  # List of leaf node hashes
        self.tree = []    # Complete tree structure

    def _hash(self, data):
        """Hash a data block using SHA-256."""
        return hashlib.sha256(data.encode('utf-8')).hexdigest()

    def add_leaf(self, key, value):
        """Add a leaf node (hash of key-value pair)."""
        leaf_hash = self._hash(f"{key}:{value}")
        self.leaves.append(leaf_hash)

    def build_tree(self):
        """Build the Merkle tree from the leaves."""
        nodes = self.leaves[:]
        while len(nodes) > 1:
            new_level = []
            for i in range(0, len(nodes), 2):
                if i + 1 < len(nodes):
                    # Combine and hash sibling nodes
                    combined_hash = self._hash(nodes[i] + nodes[i + 1])
                else:
                    # Handle odd number of nodes (duplicate last node)
                    combined_hash = self._hash(nodes[i] + nodes[i])
                new_level.append(combined_hash)
            nodes = new_level
        self.tree = nodes
        return self.tree[0] if self.tree else None  # Return root hash

    def verify_data(self, key, value):
        """Verify a key-value pair is part of the tree."""
        leaf_hash = self._hash(f"{key}:{value}")
        return leaf_hash in self.leaves
