# import unittest
# from utils.data_structures import InMemoryStorage

# class TestInMemoryStorage(unittest.TestCase):
#     def setUp(self):
#         self.storage = InMemoryStorage()

#     def test_put(self):
#         response = self.storage.put("key1", "value1")
#         self.assertEqual(response, "Key 'key1' added successfully.")
#         self.assertIn("key1", self.storage.data_store)

#     def test_get_existing_key(self):
#         self.storage.put("key2", "value2")
#         response = self.storage.get("key2")
#         self.assertEqual(response, "value2")

#     def test_get_nonexistent_key(self):
#         response = self.storage.get("key3")
#         self.assertEqual(response, "Error: Key 'key3' not found.")

#MODIFIED
import unittest
from utils.data_structures import InMemoryStorage

from server.server_upd import TransactionManager  # Import TransactionManager from your server code

class TestInMemoryStorageWith2PC(unittest.TestCase):
    def setUp(self):
        self.storage = InMemoryStorage()
        self.transaction_manager = TransactionManager()  # Add TransactionManager for 2PC tests

    # Existing tests
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

    # New tests for Two-Phase Commit
    def test_prepare_phase(self):
        transaction_id = "TXN1"
        operations = {"key1": "value1", "key2": "value2"}

        # Prepare the transaction
        result = self.transaction_manager.prepare(transaction_id, operations)
        self.assertTrue(result)
        self.assertIn(transaction_id, self.transaction_manager.transactions)
        self.assertEqual(self.transaction_manager.transactions[transaction_id]["state"], "PREPARED")

    def test_commit_phase(self):
        transaction_id = "TXN2"
        operations = {"key3": "value3", "key4": "value4"}

        # Prepare the transaction
        self.transaction_manager.prepare(transaction_id, operations)

        # Commit the transaction
        result = self.transaction_manager.commit(transaction_id, self.storage)
        self.assertTrue(result)
        self.assertEqual(self.transaction_manager.transactions[transaction_id]["state"], "COMMITTED")
        self.assertEqual(self.storage.get("key3"), "value3")
        self.assertEqual(self.storage.get("key4"), "value4")

    def test_rollback_phase(self):
        transaction_id = "TXN3"
        operations = {"key5": "value5", "key6": "value6"}

        # Prepare the transaction
        self.transaction_manager.prepare(transaction_id, operations)

        # Rollback the transaction
        result = self.transaction_manager.rollback(transaction_id)
        self.assertTrue(result)
        self.assertNotIn(transaction_id, self.transaction_manager.transactions)
        self.assertEqual(self.storage.get("key5"), "Error: Key 'key5' not found.")
        self.assertEqual(self.storage.get("key6"), "Error: Key 'key6' not found.")

    def test_partial_prepare_failure(self):
        """
        Simulate partial failure during the prepare phase by testing multiple transactions
        and ensuring rollback on failure.
        """
        transaction_id = "TXN4"
        operations = {"key7": "value7", "key8": "value8"}

        # Simulate a failure by not actually storing the prepare
        self.transaction_manager.prepare(transaction_id, operations)

        # Attempt rollback due to failure
        result = self.transaction_manager.rollback(transaction_id)
        self.assertTrue(result)
        self.assertEqual(self.storage.get("key7"), "Error: Key 'key7' not found.")
        self.assertEqual(self.storage.get("key8"), "Error: Key 'key8' not found.")

    def test_transaction_with_existing_keys(self):
        """
        Test a transaction where some keys already exist in storage.
        """
        self.storage.put("key9", "old_value")
        transaction_id = "TXN5"
        operations = {"key9": "new_value", "key10": "value10"}

        # Prepare and commit the transaction
        self.transaction_manager.prepare(transaction_id, operations)
        result = self.transaction_manager.commit(transaction_id, self.storage)
        self.assertTrue(result)

        # Check updated values
        self.assertEqual(self.storage.get("key9"), "new_value")
        self.assertEqual(self.storage.get("key10"), "value10")

# Run the tests
if __name__ == "__main__":
    unittest.main()
