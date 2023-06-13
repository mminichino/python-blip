Python BLIP
===========
Pure Python implementation of the Couchbase BLIP sync protocol.

Installing
==========
```
$ pip install pythonblip
```

Usage
=====
```
from pythonblip.headers import SessionAuth
from pythonblip.replicator import Replicator, ReplicatorConfiguration, ReplicatorType
from pythonblip.output import LocalDB, LocalFile, ScreenOutput

host = "127.0.0.1"
database = "mobile"
connect_string = f"ws://{host}:4984"
directory = os.environ['HOME']
scope = "data"
collections = ["employees", "payroll"]

replicator = Replicator(ReplicatorConfiguration.create(
    database,
    connect_string,
    ReplicatorType.PULL,
    SessionAuth(options.session),
    scope,
    collections,
    LocalFile(directory)
))

try:
    replicator.start()
    replicator.replicate()
    replicator.stop()
except Exception as err:
    print(f"Error: {err}")
```
