import hashlib

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
