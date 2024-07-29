# Files

- `main.py`: main source file. USE THIS!
- `test.py`: test pymodbus
- `server_async.py`: modbus example server from pymodbus examples.
- `helper.py`: used by `server_async.py`

# Running

Firstly, run server:

```sh
python server_async.py -c tcp -f socket -p 5050
```

Then, run `main.py`:

```sh
python main.py
```
