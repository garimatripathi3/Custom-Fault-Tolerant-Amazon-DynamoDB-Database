import threading
import time
import socket
import logging

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
    
    def redistribute_data(self, failed_node):
        logging.warning(f"Redistributing data from failed node {failed_node}")
        # Remove the failed node from consistent hashing
        self.hashing.remove_node(failed_node)
        # Iterate over data and redistribute
        for key, value in failed_node_storage.items():  # Assuming failed_node_storage holds node's data
            responsible_node = self.hashing.get_node(key)
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(responsible_node)
                    sock.sendall(f"PUT {key} {value}".encode('utf-8'))
                logging.info(f"Key '{key}' redistributed to node {responsible_node}")
            except Exception as e:
                logging.error(f"Failed to redistribute key '{key}' to node {responsible_node}: {e}")

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
