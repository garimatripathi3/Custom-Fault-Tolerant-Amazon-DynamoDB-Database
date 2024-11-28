import os
import json
import time
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO)

class BackupManager:
    def __init__(self, storage, backup_dir="backups", log_file="write_ahead_log.txt"):
        self.storage = storage
        self.backup_dir = backup_dir
        self.log_file = log_file
        self.backup_interval = 5

        # Ensure the backup directory exists
        os.makedirs(self.backup_dir, exist_ok=True)

    def save_snapshot(self):
        """Periodically save the current state of the storage to a snapshot file."""
        backup_filename = os.path.join(self.backup_dir, f"backup_{int(time.time())}.json")
        with open(backup_filename, 'w') as backup_file:
            json.dump(self.storage, backup_file)
        logging.info(f"Backup saved to {backup_filename}")

    def apply_backup(self, backup_filename):
        """Restore storage from a backup file."""
        if os.path.exists(backup_filename):
            with open(backup_filename, 'r') as backup_file:
                self.storage = json.load(backup_file)
            logging.info(f"Backup applied from {backup_filename}")
        else:
            logging.error(f"Backup file {backup_filename} does not exist.")

    def log_write(self, operation):
        """Log a write operation (e.g., PUT command) to a write-ahead log."""
        with open(self.log_file, 'a') as log_file:
            log_file.write(f"{operation}\n")
        logging.info(f"Operation logged: {operation}")

    def replay_log(self):
        """Replay the write-ahead log to restore the system's state after failure."""
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as log_file:
                for line in log_file:
                    operation = line.strip()
                    # Apply each logged operation to storage
                    self.apply_log_operation(operation)
            logging.info("Log replay completed.")
        else:
            logging.info("No log file found to replay.")

    def apply_log_operation(self, operation):
        """Apply an operation (like a PUT command) from the log to the storage."""
        parts = operation.split()
        if parts[0] == "PUT" and len(parts) == 3:
            key = parts[1]
            value = parts[2]
            self.storage[key] = value
            logging.info(f"Applied operation: {operation}")
    def periodic_backup(self):
        """Run periodic backups at the defined interval."""
        while True:
            time.sleep(self.backup_interval)  # Sleep for the backup interval
            self.save_snapshot()  #

