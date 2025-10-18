import sys
import os

# Constants
DATA_FILE = 'data.db'

# In-memory index: list of (key, value) tuples (simple linear structure, no built-in dict)
store = []

def load_store():
    """
    Replay the append-only log to rebuild the in-memory index.
    Processes each line in order; last write wins for each key.
    """
    if not os.path.exists(DATA_FILE):
        return
    with open(DATA_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            if not line.startswith('SET '):
                continue
            # Parse: split max 2 times (command, key, rest-as-value)
            parts = line.split(None, 2)
            if len(parts) < 3 or parts[0] != 'SET':
                continue
            key = parts[1]
            value = parts[2]
            # Linear scan: update if key exists, else append
            updated = False
            for i, (k, v) in enumerate(store):
                if k == key:
                    store[i] = (key, value)
                    updated = True
                    break
            if not updated:
                store.append((key, value))

def save_set(key, value):
    """
    Persist SET to disk (append-only) and update in-memory index.
    Flushes immediately for durability.
    """
    with open(DATA_FILE, 'a') as f:
        f.write(f"SET {key} {value}\n")
        f.flush()  # Ensure immediate write to disk
    # Update in-memory (same logic as load)
    updated = False
    for i, (k, v) in enumerate(store):
        if k == key:
            store[i] = (key, value)
            updated = True
            break
    if not updated:
        store.append((key, value))

def get_value(key):
    """
    Linear scan for key; return value or 'NULL' if not found.
    """
    for k, v in store:
        if k == key:
            return v
    return 'NULL'

# Main CLI loop
if __name__ == '__main__':
    # Load persisted state on startup
    load_store()
    
    # Read from STDIN line-by-line
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue  # Skip empty lines
        if line == 'EXIT':
            break  # Exit gracefully
        
        # Parse command (max 2 splits for value with spaces)
        parts = line.split(None, 2)
        if len(parts) == 2 and parts[0] == 'GET':
            # GET key
            key = parts[1]
            value = get_value(key)
            print(value)  # Output to STDOUT
        elif len(parts) >= 3 and parts[0] == 'SET':
            # SET key value (value can have spaces)
            key = parts[1]
            value = parts[2]
            save_set(key, value)
            # No output for SET
        # Ignore invalid commands