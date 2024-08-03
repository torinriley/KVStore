import json
import time
import threading
import os

class AdvancedKeyValueStore:
    def __init__(self, filename='db.json', log_filename='db_log.txt', replica=None):
        print("Initializing AdvancedKeyValueStore")
        self.store = {}
        self.ttl_store = {}
        self.index = {}
        self.filename = filename
        self.log_filename = log_filename
        self.lock = threading.Lock()
        self.replica = replica
        self.load()

    def set(self, key, value, ttl=None):
        with self.lock:
            print(f"Setting key: {key}, value: {value}, ttl: {ttl}")
            self.store[key] = value
            self.index_key(key, value)
            print("Indexed key, now saving.")
            if ttl:
                self.ttl_store[key] = time.time() + ttl
            self.save()
            print("Saved, now logging operation.")
            if self.replica:
                self.replica.set(key, value, ttl)
            self.log_operation('set', key, value, ttl)
        print("Set operation complete")

    def get(self, key):
        with self.lock:
            print(f"Getting key: {key}")
            if key in self.ttl_store and time.time() > self.ttl_store[key]:
                del self.store[key]
                del self.ttl_store[key]
                self.save()
                return None
            return self.store.get(key, None)

    def delete(self, key):
        with self.lock:
            print(f"Deleting key: {key}")
            if key in self.store:
                del self.store[key]
            if key in self.ttl_store:
                del self.ttl_store[key]
            if key in self.index:
                del self.index[key]
            self.save()
            if self.replica:
                self.replica.delete(key)
            self.log_operation('delete', key)
        print("Delete operation complete")

    def exists(self, key):
        with self.lock:
            print(f"Checking existence of key: {key}")
            if key in self.ttl_store and time.time() > self.ttl_store[key]:
                del self.store[key]
                del self.ttl_store[key]
                self.save()
                return False
            return key in self.store

    def save(self):
        with self.lock:
            print("Saving data to file")
            with open(self.filename, 'w') as f:
                json.dump({'store': self.store, 'ttl_store': self.ttl_store, 'index': self.index}, f)
        print("Save operation complete")

    def load(self):
        try:
            print("Loading data from file")
            with open(self.filename, 'r') as f:
                try:
                    data = json.load(f)
                    self.store = data.get('store', {})
                    self.ttl_store = data.get('ttl_store', {})
                    self.index = data.get('index', {})
                except json.JSONDecodeError:
                    print(f"File {self.filename} is empty or corrupted, initializing new store")
                    self.store = {}
                    self.ttl_store = {}
                    self.index = {}
        except FileNotFoundError:
            print("File not found, initializing new store")
            self.store = {}
            self.ttl_store = {}
            self.index = {}

    def index_key(self, key, value):
        print(f"Indexing key: {key}, value: {value}")
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in self.index:
                    self.index[k] = {}
                self.index[k][key] = v
        print(f"Indexing complete for key: {key}")

    def query(self, index_key, index_value):
        with self.lock:
            print(f"Querying index_key: {index_key}, index_value: {index_value}")
            return [k for k, v in self.index.get(index_key, {}).items() if v == index_value]

    def log_operation(self, op, key, value=None, ttl=None):
        print(f"Logging operation: {op}, key: {key}, value: {value}, ttl: {ttl}")
        with open(self.log_filename, 'a') as f:
            f.write(f'{op} {key} {value} {ttl}\n')
        print("Log operation complete")

class Transaction:
    def __init__(self, db):
        print("Initializing Transaction")
        self.db = db
        self.operations = []

    def set(self, key, value, ttl=None):
        print(f"Transaction set: key: {key}, value: {value}, ttl: {ttl}")
        self.operations.append(('set', key, value, ttl))

    def delete(self, key):
        print(f"Transaction delete: key: {key}")
        self.operations.append(('delete', key))

    def commit(self):
        with self.db.lock:
            print("Committing transaction")
            for op in self.operations:
                if op[0] == 'set':
                    self.db.set(op[1], op[2], op[3])
                elif op[0] == 'delete':
                    self.db.delete(op[1])
            self.db.save()
        print("Transaction commit complete")

def main():
    while True:
        command = input("Enter command (set/get/delete/exists/query/transaction/create_file/write_file/download_file/exit): ").strip().lower()
        print(f"Received command: {command}")

        if command == 'set':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            key = input("Enter key: ").strip()
            value = input("Enter value: ").strip()
            ttl = input("Enter TTL (in seconds, optional): ").strip()
            ttl = int(ttl) if ttl else None
            db.set(key, value, ttl)
            print(f"Set {key} = {value} with TTL = {ttl}")

        elif command == 'get':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            key = input("Enter key: ").strip()
            value = db.get(key)
            if value is not None:
                print(f"Get {key} = {value}")
            else:
                print(f"{key} not found or expired")

        elif command == 'delete':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            key = input("Enter key: ").strip()
            db.delete(key)
            print(f"Deleted {key}")

        elif command == 'exists':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            key = input("Enter key: ").strip()
            exists = db.exists(key)
            print(f"{key} exists: {exists}")

        elif command == 'query':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            index_key = input("Enter index key: ").strip()
            index_value = input("Enter index value: ").strip()
            results = db.query(index_key, index_value)
            print(f"Query results for {index_key} = {index_value}: {results}")

        elif command == 'transaction':
            filename = input("Enter filename to use for key-value store: ").strip()
            db = AdvancedKeyValueStore(filename)
            trans = Transaction(db)
            while True:
                t_command = input("Enter transaction command (set/delete/commit/abort): ").strip().lower()
                print(f"Received transaction command: {t_command}")
                if t_command == 'set':
                    key = input("Enter key: ").strip()
                    value = input("Enter value: ").strip()
                    ttl = input("Enter TTL (in seconds, optional): ").strip()
                    ttl = int(ttl) if ttl else None
                    trans.set(key, value, ttl)
                    print(f"Transaction set {key} = {value} with TTL = {ttl}")
                elif t_command == 'delete':
                    key = input("Enter key: ").strip()
                    trans.delete(key)
                    print(f"Transaction deleted {key}")
                elif t_command == 'commit':
                    trans.commit()
                    print("Transaction committed")
                    break
                elif t_command == 'abort':
                    print("Transaction aborted")
                    break
                else:
                    print("Unknown transaction command")

        elif command == 'create_file':
            filename = input("Enter file name: ").strip()
            try:
                with open(filename, 'w') as file:
                    print(f"File '{filename}' created successfully.")
                db = AdvancedKeyValueStore(filename)
            except Exception as e:
                print(f"Error creating file '{filename}': {e}")

        elif command == 'write_file':
            filename = input("Enter file name: ").strip()
            content = input("Enter content to write: ").strip()
            try:
                with open(filename, 'a') as file:
                    file.write(content + '\n')
                    print(f"Content written to '{filename}' successfully.")
            except Exception as e:
                print(f"Error writing to file '{filename}': {e}")

        elif command == 'download_file':
            filename = input("Enter file name to download: ").strip()
            try:
                if os.path.exists(filename):
                    print(f"File '{filename}' is ready for download. Path: {os.path.abspath(filename)}")
                else:
                    print(f"File '{filename}' does not exist.")
            except Exception as e:
                print(f"Error accessing file '{filename}': {e}")

        elif command == 'exit':
            print("Exiting...")
            break

        else:
            print("Unknown command")

if __name__ == "__main__":
    main()
