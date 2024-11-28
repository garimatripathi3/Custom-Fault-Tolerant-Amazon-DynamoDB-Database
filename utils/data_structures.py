import json
import os
import logging
import json
import os

class PersistentStorage:
    def __init__(self, storage_file="server_storage.json"):
        self.storage_file = storage_file
        self.data = self.load_data()

    def load_data(self):
        """Load the data from the storage file, handling empty files gracefully."""
        if not os.path.exists(self.storage_file) or os.path.getsize(self.storage_file) == 0:
            # If the file doesn't exist or is empty, return an empty dictionary
            return {}
        
        try:
            with open(self.storage_file, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            # Handle the case where the file exists but the contents are invalid
            print(f"Error: Failed to decode JSON from {self.storage_file}. Returning empty storage.")
            return {}

    def save_data(self):
        """Save the data to the storage file."""
        with open(self.storage_file, "w") as f:
            json.dump(self.data, f)

    def __getitem__(self, key):
        """Allow accessing the data like a dictionary."""
        return self.data.get(key)

    def __setitem__(self, key, value):
        """Allow setting data like a dictionary."""
        self.data[key] = value
        self.save_data()  # Persist data after every change

    def __delitem__(self, key):
        """Allow deleting an item from the storage."""
        if key in self.data:
            del self.data[key]
            self.save_data()  # Persist data after deletion

    def __contains__(self, key):
        """Allow checking if a key exists in the storage."""
        return key in self.data

# class PersistentStorage:
#     def __init__(self, storage_file="server_storage.json"):
#         self.storage_file = storage_file
#         self.data = self.load_data()

#     def load_data(self):
#         """Load the data from the storage file, handling empty files gracefully."""
#         if not os.path.exists(self.storage_file) or os.path.getsize(self.storage_file) == 0:
#             return {}
#         try:
#             with open(self.storage_file, "r") as f:
#                 return json.load(f)
#         except json.JSONDecodeError:
#             print(f"Error: Failed to decode JSON from {self.storage_file}. Returning empty storage.")
#             return {}

#     def save_data(self):
#         """Save the data to the storage file."""
#         with open(self.storage_file, "w") as f:
#             json.dump(self.data, f)

#     def __getitem__(self, key):
#         """Allow accessing the data like a dictionary."""
#         return self.data.get(key)

#     def __setitem__(self, key, value):
#         """Allow setting data like a dictionary."""
#         self.data[key] = value
#         self.save_data()

#     def __delitem__(self, key):
#         """Allow deleting an item from the storage."""
#         if key in self.data:
#             del self.data[key]
#             self.save_data()

#     def __contains__(self, key):
#         """Allow checking if a key exists in the storage."""
#         return key in self.data

#     def put(self, key, value):
#         """Add or update an item in storage."""
#         self[key] = value

# class PersistentStorage:
#     def __init__(self, storage_file):
#         self.storage_file = storage_file
#         self.data_store = self._load_from_file()

#     def _load_from_file(self):
#         """Load key-value data from the file."""
#         if os.path.exists(self.storage_file):
#             try:
#                 with open(self.storage_file, "r") as file:
#                     return json.load(file)
#             except (IOError, json.JSONDecodeError) as e:
#                 logging.error(f"Error loading storage file: {e}")
#                 return {}
#         return {}

#     def _save_to_file(self):
#         """Save key-value data to the file."""
#         try:
#             with open(self.storage_file, "w") as file:
#                 json.dump(self.data_store, file)
#         except IOError as e:
#             logging.error(f"Error saving to storage file: {e}")

#     def put(self, key, value):
#         """Store the key-value pair and persist to disk."""
#         self.data_store[key] = value
#         self._save_to_file()
#         return f"Key '{key}' added successfully."

#     def get(self, key):
#         """Retrieve the value for the given key."""
#         if key in self.data_store:
#             return self.data_store[key]
#         else:
#             return f"Error: Key '{key}' not found."

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
# Modified
