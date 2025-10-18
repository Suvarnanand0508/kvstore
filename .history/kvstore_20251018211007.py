import sys
import os

# Constants
DATA_FILE = 'data.db'

# The in-memory index: Switched from list (O(N) lookup) to dict (O(1) lookup)
# This maps the key string to its current value string.
store = {}

def load_store():
    """
    Replay the append-only log file to rebuild the in-memory index (hash map).
    Since we process lines sequentially, the last recorded 'SET' for a key
    will automatically overwrite previous entries, correctly implementing
    the "last write wins" recovery strategy.
    """
    if not os.path.exists(DATA_FILE):
        return

    print(f"Loading state from {DATA_FILE}...", file=sys.stderr)
    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                # We only care about SET commands for reconstruction
                if line.startswith('SET '):
                    # Parse: split max 2 times (command, key, rest-as-value)
                    parts = line.split(None, 2)
                    
                    if len(parts) == 3 and parts[0] == 'SET':
                        key = parts[1]
                        value = parts[2]
                        # O(1) update/insertion in the dictionary
                        store[key] = value
        print("Load complete.", file=sys.stderr)
    except IOError as e:
        # Handle cases where the file might be corrupted or inaccessible
        print(f"Error reading data file: {e}", file=sys.stderr)

def save_set(key, value):
    """
    Persist SET to disk (append-only log) and update the O(1) in-memory index.
    Flushes immediately for durability (Write Ahead Log behavior).
    """
    try:
        # 1. Write to the append-only log file
        with open(DATA_FILE, 'a') as f:
            f.write(f"SET {key} {value}\n")
            f.flush()  # Ensure immediate write to disk for durability

        # 2. Update in-memory index (O(1) operation)
        store[key] = value
        
    except IOError as e:
        print(f"Error writing to data file: {e}", file=sys.stderr)

def get_value(key):
    """
    Retrieve value using the O(1) in-memory dictionary lookup.
    """
    # Use .get() to return 'NULL' if the key is not found
    return store.get(key, 'NULL')

# Main CLI loop
if __name__ == '__main__':
    # Load persisted state on startup
    load_store()
    
    # Read from STDIN line-by-line
    # Note: Using sys.stdin is excellent for pipe-based or interactive CLI usage
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue  # Skip empty lines
            
        # Standard exit command
        if line == 'EXIT':
            break 
            
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
            # No output for SET, as is common in K/V stores
            
        # Invalid commands are silently ignored
