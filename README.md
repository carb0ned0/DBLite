# DBLite

DBLite is a lightweight, in-memory, Redis-like key-value database server and client written in pure Python. It supports common data structures like strings, lists, hashes, and sets, along with key expiry and disk persistence.

It's built with the standard `socketserver` library for multi-threading and can be optionally super-charged with `gevent` for high-performance, concurrent I/O.

## Key Features

* Zero-dependency pure Python server (when not using gevent).
* Multi-threaded request handling.
* Optional `gevent` support for high-concurrency.
* Data persistence (`SAVE`/`RESTORE`) using `pickle`.
* Key expiry (`EXPIRE`).
* Rich data types:
  * Strings (KV): `SET`, `GET`
  * Lists (Queues): `LPUSH`, `LPOP`
  * Hashes (Dicts): `HSET`, `HGET`
  * Sets: `SADD`, `SMEMBERS`
* Redis-like protocol for client/server communication.

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/carb0ned0/dblite.git
    cd dblite
    ```

2. Install the package using pip. This will also set up the dblite-server command.

    ```bash
    pip install .
    ```

3. For High-Performance Concurrency (Optional): To use gevent (which the server will automatically detect and use if installed), install the gevent extra:

    ```bash
    pip install .[gevent]
    ```

## Usage

1. **Running the Server**

    Once installed, you can start the server from your terminal using the `dblite-server` command.

    ```bash
    # Start the server on the default host and port (127.0.0.1:31337)
    dblite-server
    ```

    The server will log that it has started: `INFO:__main__:Starting DBLite server on 127.0.0.1:31337`

    You can also specify a host and port:

    ```bash
    dblite-server --host 0.0.0.0 --port 6379
    ```

2. **Using the Client**

    You can interact with the server using the Client class in any Python script or shell.

    ```python
    from dblite import Client

    # Connect to the running server
    c = Client(host='127.0.0.1', port=31337)

    # --- 1. Strings (Key-Value) ---
    print(c.set('user:1', 'FellowHuman'))
    # 'OK'

    print(c.get('user:1'))
    # 'FellowHuman'

    print(c.exists('user:1'))
    # 1

    print(c.delete('user:1'))
    # 1

    print(c.get('user:1'))
    # None

    # --- 2. Lists (Queues) ---
    # Use LPUSH to add items. It acts like a stack (Last-In, First-Out).
    print(c.lpush('my_tasks', 'task_3', 'task_2', 'task_1'))
    # 3 (current size of the list)

    print(c.lpop('my_tasks'))
    # 'task_1'
    print(c.lpop('my_tasks'))
    # 'task_2'

    # --- 3. Hashes (Dicts) ---
    print(c.hset('user:profile', 'name', 'FellowHuman'))
    # 1
    print(c.hset('user:profile', 'github', 'your-username'))
    # 1

    print(c.hget('user:profile', 'name'))
    # 'FellowHuman'

    # --- 4. Sets (Unique, unordered collections) ---
    print(c.sadd('friends:1', 'bob', 'alice', 'charlie'))
    # 3 (number of items added)

    print(c.sadd('friends:1', 'alice')) # 'alice' is already in the set
    # 0 (no new items added)

    print(c.smembers('friends:1'))
    # {'alice', 'bob', 'charlie'}

    # --- 5. Expiry ---
    print(c.set('temp_key', 'this will vanish'))
    # 'OK'
    print(c.expire('temp_key', 2)) # Expire in 2 seconds
    # 1

    import time
    time.sleep(3)

    print(c.get('temp_key'))
    # None (the key has expired and is gone)
    ```

## Persistence (SAVE / RESTORE)

You can save the entire in-memory database to a file and restore it later.

```python

# Save all current data to a file named 'my_data.db'
c.save('my_data.db')
# 'OK'

# Clear all keys from memory
c.flushall()
# 'OK'

print(c.get('user:1'))
# None

# Restore the database from the file
c.restore('my_data.db')
# 1 (success)

print(c.get('user:1'))
# 'FellowHuman'
```

Security Warning: The persistence feature uses Python's `pickle` module. This is not secure against maliciously crafted data. Only load dump files (`.db`) from trusted sources.

## Command Reference

Here is a full list of commands supported by the server.

Command | Arguments | Description
--- | --- | ---
`SET` | `key, value` | Sets the string value of a key.
`GET` | `key` | Gets the string value of a key.
`DELETE` | `key` | Deletes a key.
`EXISTS` | `key` | Checks if a key exists.
`LPUSH` | `key, value1, [value2...]` | Prepends one or more values to a list.
`LPOP` | `key` | Removes and returns the first element of a list.
`HSET` | `key, field, value` | Sets a field in a hash to a value.
`HGET` | `key, field` | Gets the value of a field in a hash.
`SADD` | `key, member1, [member2...]` | Adds one or more members to a set.
`SMEMBERS` | `key` | Gets all members of a set.
`EXPIRE` | `key, seconds` | Sets a timeout (in seconds) on a key.
`FLUSHALL` | - | Deletes all keys from the database.
`SAVE` | `filename` | Saves the current database state to disk.
`RESTORE` | `filename` | Loads a database state from disk.
`INFO` | - | Returns a dictionary of server stats.
`QUIT` | - | Disconnects the client.
`SHUTDOWN` | - | Shuts down the server.

## Running Tests

The project includes a full test suite that verifies all functionality. The test script will automatically start the server, run all tests, and then shut it down.

1. Make sure you have the server running or stopped (the test suite will manage it).
2. Run the test file:

```bash
python test_dblite.py
```

You should see an OK message indicating all tests have passed.

```bash
..........
----------------------------------------------------------------------
Ran 10 tests in 2.577s

OK
```

## License

This project is licensed under the MIT License.
