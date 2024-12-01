import hashlib
import os
import fnmatch
import shutil
import json
class ConsistentHashing:
    def __init__(self, nodes, replicas=3):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        for i in range(self.replicas):
            key = self.hash(f"{node}-{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def remove_node(self, node):
        for i in range(self.replicas):
            key = self.hash(f"{node}-{i}")
            del self.ring[key]
            self.sorted_keys.remove(key)

    def hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def get_node(self, key):
        hash_key = self.hash(key)
        for node_key in self.sorted_keys:
            if hash_key <= node_key:
                return self.ring[node_key]
        return self.ring[self.sorted_keys[0]]

    def get_replicas(self, key):
        node = self.get_node(key)
        index = self.sorted_keys.index(self.hash(f"{node}-0"))
        replicas = []
        for i in range(self.replicas):
            replicas.append(self.ring[self.sorted_keys[(index + i) % len(self.sorted_keys)]])
        return replicas
    
    def get_keys_responsible(node):
        node_port = node[1]
        
    
    def find_and_transfer_json_files(directory, node_port, replica_port):
        file_found = False
        for root, _, files in os.walk(directory):
            for file in files:
                if fnmatch.fnmatch(file, f"*{node_port}*.json"):  # Look for files containing node_port
                    file_found = True
                    old_path = os.path.join(root, file)
                    
                    # Load data from the file
                    with open(old_path, 'r') as json_file:
                        try:
                            data = json.load(json_file)  # Assuming JSON content is valid
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON from file {file}: {e}")
                            continue

                    # Construct new file name for the replica
                    new_file_name = file.replace(node_port, replica_port)
                    new_path = os.path.join(root, new_file_name)

                    # Write data to the new file
                    with open(new_path, 'w') as new_json_file:
                        json.dump(data, new_json_file, indent=4)

                    print(f"Data copied from {old_path} to {new_path}")

                    # Optional: Remove the old file if needed
                    # os.remove(old_path)
        
        if not file_found:
            print(f"No JSON file containing port {node_port} found.")
        return file_found
