import unittest
from utils.data_structures import InMemoryStorage

class TestInMemoryStorage(unittest.TestCase):
    def setUp(self):
        self.storage = InMemoryStorage()

    def test_put(self):
        response = self.storage.put("key1", "value1")
        self.assertEqual(response, "Key 'key1' added successfully.")
        self.assertIn("key1", self.storage.data_store)

    def test_get_existing_key(self):
        self.storage.put("key2", "value2")
        response = self.storage.get("key2")
        self.assertEqual(response, "value2")

    def test_get_nonexistent_key(self):
        response = self.storage.get("key3")
        self.assertEqual(response, "Error: Key 'key3' not found.")
