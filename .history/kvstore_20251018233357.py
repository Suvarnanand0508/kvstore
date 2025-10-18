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
                    if len(parts) == 3 and parts[0] == 'SET':
                        key, value = parts[1], parts[2]
                        store[key] = value
    except IOError as e:
        print(f"Error reading data file: {e}", file=sys.stderr)

def save_set(key, value):
    try:
        with open(DATA_FILE, 'a') as f:
            f.write(f"SET {key} {value}\n")
            f.flush()
            os.fsync(f.fileno())
        store[key] = value
    except IOError as e:
        print(f"Error writing to data file: {e}", file=sys.stderr)

def get_value(key):
    return store.get(key, 'NULL')

if __name__ == '__main__':
    load_store()

    # Detect if input is interactive (terminal) or piped/scripted
    interactive = sys.stdin.isatty()

    if interactive:
        print("--- Simple Key-Value Store ---", file=sys.stderr)
        print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)

    while True:
        try:
            # Only show prompt if user is interactive
            line = input("db> " if interactive else "")
        except (EOFError, KeyboardInterrupt):
            break

        line = line.strip()
        if not line:
            continue

        if line == 'EXIT':
            break

        parts = line.split(None, 2)

        if len(parts) == 2 and parts[0] == 'GET':
            print(get_value(parts[1]))

        elif len(parts) >= 3 and parts[0] == 'SET':
            save_set(parts[1], parts[2])
            if interactive:
                print("(OK)", file=sys.stderr)
