import sys
import os

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────
DATA_FILE = 'data.db'  # Append-only persistence file
CMD_SET = 'SET'
CMD_GET = 'GET'
CMD_EXIT = 'EXIT'

# In-memory key-value store (O(1) lookups)
store = {}


# ─────────────────────────────────────────────────────────────
# Persistence & Data Loading Functions
# ─────────────────────────────────────────────────────────────
def load_store():
    """
    Load the persistent log file into memory.
    Only 'SET <key> <value>' lines are applied.
    This replays history; last write wins.
    """
    if not os.path.exists(DATA_FILE):
        return  # No previous data — nothing to load

    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{CMD_SET} "):
                    parts = line.split(None, 2)
                    if len(parts) == 3:
                        key, value = parts[1], parts[2]
                        store[key] = value  # Overwrite if key exists
    except (IOError, OSError) as e:
        print(f"Error loading data file: {e}", file=sys.stderr)


def save_set(key, value):
    """
    Persist a SET operation to disk (append-only).
    Updates in-memory store after writing to file.

    Uses flush() and fsync() to ensure data is physically written.
    """
    try:
        with open(DATA_FILE, 'a') as f:
            f.write(f"{CMD_SET} {key} {value}\n")
            f.flush()            # Write to OS buffer
            os.fsync(f.fileno()) # Force write to disk

        store[key] = value       # Update in-memory
    except (IOError, OSError) as e:
        print(f"Error writing to data file: {e}", file=sys.stderr)


def get_value(key):
    """
    Retrieve the value for a key from memory.
    Returns:
        - The stored string value if key exists
        - None if key does not exist (test expects empty output)
    """
    return store.get(key)


# ─────────────────────────────────────────────────────────────
# Main CLI Loop
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    load_store()

    # Detect interactive vs automated mode (e.g. piping input in tests)
    interactive = sys.stdin.isatty()

    if interactive:
        print("--- Simple Key-Value Store ---", file=sys.stderr)
        print("Commands: SET <key> <value>, GET <key>, EXIT", file=sys.stderr)

    while True:
        try:
            # Show prompt only in interactive mode
            line = input("db> " if interactive else "")
        except (EOFError, KeyboardInterrupt):
            break  # Exit gracefully on Ctrl+D or Ctrl+C

        if not line:
            continue  # Ignore empty input

        line = line.strip()

        if line == CMD_EXIT:
            break  # Stop execution

        parts = line.split(None, 2)

        # ──────────────────────────────────────────────
        # GET <key>
        # ──────────────────────────────────────────────
        if len(parts) == 2 and parts[0] == CMD_GET:
            value = get_value(parts[1])
            if value is not None:
                print(value)  # Key found → print value
            else:
                print("")     # Key not found → print empty (not "NULL")

        # ──────────────────────────────────────────────
        # SET <key> <value>
        # ──────────────────────────────────────────────
        elif len(parts) >= 3 and parts[0] == CMD_SET:
            key, value = parts[1], parts[2]
            save_set(key, value)
            if interactive:
                print("(OK)", file=sys.stderr)

        # Ignore invalid/unknown commands silently to match expected behavior
