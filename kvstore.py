import sys
import os

DATA_FILE = 'data.db'
store = {}

def load_store():
    if not os.path.exists(DATA_FILE):
        return
    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('SET '):
                    parts = line.split(None, 2)
                    if len(parts) == 3:
                        store[parts[1]] = parts[2]
    except:
        pass

def save_set(key, value):
    try:
        with open(DATA_FILE, 'a') as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())
        store[key] = value
    except:
        pass

def get_value(key):
    return store.get(key, None)  # Return None instead of "NULL"

if __name__ == '__main__':
    load_store()
    interactive = sys.stdin.isatty()

    while True:
        try:
            line = input("db> " if interactive else "")
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue
        line = line.strip()

        if line == "EXIT":
            break

        parts = line.split(None, 2)

        if len(parts) == 2 and parts[0] == "GET":
            value = get_value(parts[1])
            if value is not None:
                print(value)     # Key exists → print value
            else:
                print("")        # Key missing → print empty line (NOT "NULL")

        elif len(parts) >= 3 and parts[0] == "SET":
            save_set(parts[1], parts[2])
