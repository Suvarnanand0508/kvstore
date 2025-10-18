import sys
import os

# Constants
DATA_FILE = 'data.db'

# The in-memory index: Switched from list (O(N) lookup) to dict (O(1) lookup)
# This maps the key string to its current value string.
store = {}

def load_store(verbose=True):
    """
    Replay the append-only log file to rebuild the in-memory index (hash map).
    Since we process lines sequentially, the last recorded 'SET' for a key
    will automatically overwrite previous entries, correctly implementing
    the "last write wins" recovery strategy.
    
    :param verbose: If True, print loading progress messages to stderr.
    """
    if not os.path.exists(DATA_FILE):
        return

    if verbose:
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
        if verbose:
            print("Load complete.", file=sys.stderr)
    except IOError as e:
        # Handle cases where the file might be corrupted or inaccessible
        print(f"Error reading data file: {e}", file=sys.stderr)

def save_set(key, value):
    """
    Persist SET to disk (append-only log) and update the O(1) in-memory index.
    Uses flush() and fsync() for maximum durability assurance.
    
    :param key: The key to set.
    :param value: The value to associate with the key.
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

def get_value(key):
    """
    Retrieve value using the O(1) in-memory dictionary lookup.
    
    :param key: The key to retrieve.
    :return: The value if key exists, otherwise 'NULL'.
    """
    # Use .get() to return 'NULL' if the key is not found
    return store.get(key, 'NULL')

if __name__ == '__main__':
    """
    Main entry point for the key-value store CLI.
    
    - Determines if running in interactive mode (via sys.stdin.isatty()).
    - Loads persisted state from the data file on startup.
    - In interactive mode: Displays welcome message and prompts on stderr.
    - Processes commands from stdin: SET <key> <value>, GET <key>, EXIT.
    - For SET: Persists to disk and updates in-memory store; prints (OK) on stderr if interactive.
    - For GET: Prints value (or NULL) to stdout.
    - Ignores invalid commands; prints error on stderr if interactive.
    - Handles EOF (Ctrl+D) or EXIT to terminate gracefully.
    - Ensures clean stdout in batch mode for automated testing/grading.
    """
    
    # Determine if interactive mode
    interactive = sys.stdin.isatty()
    
    # Load persisted state on startup (verbose only in interactive mode)
    load_store(interactive)

    # Welcome message and instructions (only in interactive mode)
    if interactive:
        print("--- Simple Key-Value Store (In-Memory Index) ---", file=sys.stderr)
        print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)
    
    # CLI loop: Read from stdin, process commands
    while True:
        # Prompt on stderr only in interactive mode
        if interactive:
            sys.stderr.write("db> ")
            sys.stderr.flush()
        
        # Read line (use readline for consistency across modes)
        line = sys.stdin.readline().rstrip('\n')
        if not line:
            # EOF or empty input: exit
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
            print(value)  # Output to STDOUT
            
        elif len(parts) >= 3 and parts[0] == 'SET':
            # SET key value (value can have spaces)
            key = parts[1]
            value = parts[2]
            save_set(key, value)
            # Confirmation output to stderr only in interactive mode
            if interactive:
                print("(OK)", file=sys.stderr)
                
        else:
            # Invalid command: ignore but notify in interactive mode
            if interactive:
                print("Invalid command. Use SET <key> <value>, GET <key>, or EXIT.", file=sys.stderr)