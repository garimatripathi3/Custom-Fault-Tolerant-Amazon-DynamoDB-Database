import json
import os
import logging

class PersistentStorage:
    def __init__(self, storage_file):
        self.storage_file = storage_file
        self.data_store = self._load_from_file()

    def _load_from_file(self):
        """Load key-value data from the file."""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, "r") as file:
                    return json.load(file)
            except (IOError, json.JSONDecodeError) as e:
                logging.error(f"Error loading storage file: {e}")
                return {}
        return {}

    def _save_to_file(self):
        """Save key-value data to the file."""
        try:
            with open(self.storage_file, "w") as file:
                json.dump(self.data_store, file)
        except IOError as e:
            logging.error(f"Error saving to storage file: {e}")

    def put(self, key, value):
        """Store the key-value pair and persist to disk."""
        self.data_store[key] = value
        self._save_to_file()
        return f"Key '{key}' added successfully."

    def get(self, key):
        """Retrieve the value for the given key."""
        if key in self.data_store:
            return self.data_store[key]
        else:
            return f"Error: Key '{key}' not found."

class InMemoryStorage:
    def __init__(self):
        self.data_store = {}

    def put(self, key, value):
        self.data_store[key] = value
        return f"Key '{key}' added successfully."

    def get(self, key):
        if key in self.data_store:
            return self.data_store[key]
        else:
            return f"Error: Key '{key}' not found."
