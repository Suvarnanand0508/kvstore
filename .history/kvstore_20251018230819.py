import sys
import os
from typing import Dict, Any

# Constants
DATA_FILE = 'data.db'

# The in-memory index: Switched from list (O(N) lookup) to dict (O(1) lookup)
# This maps the key string to its current value string.
store: Dict[str, str] = {}

def load_store() -> None:
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

def save_set(key: str, value: str) -> None:
    """
    Persist SET to disk (append-only log) and update the O(1) in-memory index.
    Uses flush() and fsync() for maximum durability assurance.
    """
    try:
        # 1. Write to the append-only log file
        with open(DATA_FILE, 'a') as f:
            f.write(f"SET {key} {value}\n")
            f.flush()  # Ensure data is sent to the operating system buffer
            os.fsync(f.fileno()) # Force the operating system to write data to disk

        # 2. Update in-memory index (O(1) operation)
        store[key] = value
        
    except IOError as e:
        print(f"Error writing to data file: {e}", file=sys.stderr)

def get_value(key: str) -> str:
    """
    Retrieve value using the O(1) in-memory dictionary lookup.
    Returns 'NULL' if the key is not found.
    """
    # Use .get() to return 'NULL' if the key is not found
    return store.get(key, 'NULL')

# Main CLI loop
if __name__ == '__main__':
    # Load persisted state on startup
    load_store()

    # Welcome message and instructions printed to stderr to avoid mixing with stdout output
    print("--- Simple Key-Value Store (In-Memory Index) ---", file=sys.stderr)
    print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)
    
    # Use explicit print/readline for robust interactive CLI (prompts on stderr, output on stdout)
    while True:
        try:
            # 1. Display the prompt explicitly to stderr
            sys.stderr.write("db> ")
            sys.stderr.flush()
            
            # 2. Read the line from stdin
            line = sys.stdin.readline()
            
            if not line:
                break # EOF (e.g., Ctrl+D or end of a pipe)
                
        except KeyboardInterrupt:
            # Handle Ctrl+C
            break

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
            print(value)  # Output the value *only* to STDOUT
            
        elif len(parts) >= 3 and parts[0] == 'SET':
            # SET key value (value can have spaces)
            key = parts[1]
            value = parts[2]
            save_set(key, value)
            # SET is silent (no output)
            
        # Ignore invalid commands
