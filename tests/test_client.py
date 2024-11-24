import unittest
from client.client import Client

class TestClientServer(unittest.TestCase):
    def setUp(self):
        self.client = Client()

    def test_put_request(self):
        response = self.client.send_request("PUT key1 value1")
        self.assertEqual(response, "Key 'key1' added successfully.")

    def test_get_request(self):
        self.client.send_request("PUT key1 value1")
        response = self.client.send_request("GET key1")
        self.assertEqual(response, "value1")
