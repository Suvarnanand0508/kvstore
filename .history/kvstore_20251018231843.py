import sys
import os

# --- Constants ---
DATA_FILE = 'data.db'

# --- Global State (In-Memory Index) ---
# The in-memory index (hash map). This maps the key string to its current value string.
# Using a dictionary provides O(1) average-case time complexity for lookups, insertions, and updates.
store = {}

def load_store():
    """
    Replay the append-only log file (data.db) to rebuild the in-memory index (hash map).

    Since the log is processed sequentially, the last recorded 'SET' command for a
    given key will naturally overwrite previous entries, correctly implementing
    the "last write wins" recovery strategy upon startup.
    """
    if not os.path.exists(DATA_FILE):
        return

    print(f"Loading state from {DATA_FILE}...", file=sys.stderr)
    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                # Only 'SET' commands are needed for state reconstruction
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

def save_set(key: str, value: str):
    """
    Persist a SET command to the append-only log file and update the in-memory index.

    This function ensures durability by explicitly flushing the OS buffer and
    forcing the write to disk using os.fsync().
    
    Args:
        key: The key string to store.
        value: The value string associated with the key.
    """
    try:
        # 1. Write to the append-only log file (using 'a' for append mode)
        with open(DATA_FILE, 'a') as f:
            f.write(f"SET {key} {value}\n")
            f.flush()            # Ensure data is sent to the operating system buffer
            os.fsync(f.fileno()) # Force the operating system to write data to disk

        # 2. Update in-memory index (O(1) operation)
        store[key] = value
        
    except IOError as e:
        print(f"Error writing to data file: {e}", file=sys.stderr)

def get_value(key: str) -> str | None:
    """
    Retrieve value using the O(1) in-memory dictionary lookup.
    
    Args:
        key: The key string to retrieve.

    Returns:
        The value string if the key exists, or None if it does not.
    """
    # FIX: Change default return from 'NULL' to None.
    # This allows the main loop to print an empty line for non-existent keys,
    # satisfying the 'NonexistentGet' test requirement for an empty response.
    return store.get(key, None)

# --- Main CLI loop ---
if __name__ == '__main__':
    """
    The main interactive command-line interface loop.
    It loads the persisted state on startup and continuously processes user input
    for GET, SET, and EXIT commands.
    """
    # Load persisted state on startup
    load_store()

    # Welcome message and instructions
    print("--- Simple Key-Value Store (In-Memory Index) ---", file=sys.stderr)
    print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)
    
    # Use a while loop with input() for proper interactive prompting
    while True:
        try:
            # Display the prompt 'db> ' and wait for user input
            line = input("db> ")
        except (EOFError, KeyboardInterrupt):
            # Handle Ctrl+D (end of file) and Ctrl+C gracefully
            break

        line = line.strip()
        if not line:
            continue  # Skip empty lines
            
        # Standard exit command
        if line == 'EXIT':
            break  
            
        # Parse command (max 2 splits for value with spaces)
        parts = line.split(None, 2)
        
        command = parts[0] if parts else ''
        
        if command == 'GET' and len(parts) == 2:
            # GET key
            key = parts[1]
            value = get_value(key)
            
            # FIX: Only print the value if it exists.
            # If value is None (non-existent key), print an empty line,
            # which is achieved by simply not printing anything to stdout.
            if value is not None:
                print(value)  # Output to STDOUT
            
        elif command == 'SET' and len(parts) >= 3:
            # SET key value (value can have spaces)
            key = parts[1]
            value = parts[2]
            save_set(key, value)
            # Added confirmation output to stderr to show the command succeeded
            print("(OK)", file=sys.stderr)
            
        else:
            # Optional: provide feedback for invalid commands
            print(f"ERROR: Invalid command or arguments: '{line}'", file=sys.stderr)