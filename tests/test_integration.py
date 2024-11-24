import unittest
from client.client import Client
from utils.hashing import ConsistentHashing
from server.health_monitor import HealthMonitor

class TestReplication(unittest.TestCase):
    def setUp(self):
        # Client for Node 1 (primary)
        self.client_primary = Client(host="127.0.0.1", port=5000)
        # Client for Node 2 (replica)
        self.client_replica = Client(host="127.0.0.1", port=5001)

    def test_put_replication(self):
        # PUT operation on the primary node
        response = self.client_primary.put("key1", "value1")
        self.assertEqual(response, "Key 'key1' added successfully.")

        # GET operation from the replica node
        replica_response = self.client_replica.get("key1")
        self.assertEqual(replica_response, "value1")

    def test_get_nonexistent_key_from_replica(self):
        # GET operation for a key that doesn't exist
        response = self.client_replica.get("key2")
        self.assertEqual(response, "Error: Key 'key2' not found.")

class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.nodes = [("127.0.0.1", 5000), ("127.0.0.1", 5001), ("127.0.0.1", 5002)]
        self.hashing = ConsistentHashing(self.nodes)

    def test_consistent_hashing(self):
        key = "test_key"
        node = self.hashing.get_node(key)
        self.assertIn(node, self.nodes)

    def test_redundancy(self):
        key = "test_key"
        replicas = self.hashing.get_replicas(key)
        self.assertEqual(len(replicas), 3)
        for replica in replicas:
            self.assertIn(replica, self.nodes)

class TestErrorHandlingAndRecovery(unittest.TestCase):
    def setUp(self):
        self.primary_client = Client(host="127.0.0.1", port=5000)
        self.replica_client = Client(host="127.0.0.1", port=5001)
        self.monitor = HealthMonitor([("127.0.0.1", 5000), ("127.0.0.1", 5001)])

    def test_put_with_replication_failure(self):
        response = self.primary_client.put("key1", "value1")
        self.assertIn("Key 'key1' added successfully.", response)

        # Simulate replica failure and verify recovery
        self.monitor.redistribute_data(("127.0.0.1", 5001))
        replica_response = self.replica_client.get("key1")
        self.assertEqual(replica_response, "value1")

    def test_get_from_recovered_node(self):
        # Simulate node failure
        self.monitor.redistribute_data(("127.0.0.1", 5001))

        # Verify data availability on a healthy node
        response = self.primary_client.get("key1")
        self.assertEqual(response, "value1")