Python BLIP
===========
Pure Python implementation of the Couchbase BLIP sync protocol.

Installing
==========
```
python3 -m venv venv
```
```
. venv/bin/activate
```
```
pip3 install pythonblip
```

Usage
=====
```
from pythonblip.headers import SessionAuth
from pythonblip.replicator import Replicator, ReplicatorConfiguration, ReplicatorType
from pythonblip.output import LocalDB, LocalFile, ScreenOutput

host = "127.0.0.1"
database = "mobile"
port = "4984"
ssl = True
directory = os.environ['HOME']
scope = "data"
collections = ["employees", "payroll"]

replicator = Replicator(ReplicatorConfiguration.create(
    database,
    host,
    ReplicatorType.PULL,
    SessionAuth(options.session),
    ssl,
    port,
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

Sync documents with 3.0 and earlier protocol (all documents in the _default scope and collection).
```
blipctl -n 127.0.0.1 -d database -t 9ec978de8f0fc172708cdbb9fc3f903a882883ec -f -D /home/sync/tests/output/ --ssl
```

Use new 3.1 and later style with scopes and collections.
```
blipctl -n 127.0.0.1 -d database -t 1eaccb9f9dc219a1b5d18e18e7f3d058efd1e9ff -s scope -c collection1,collection2,collection3 -f -D /home/sync/tests/output/ --ssl
```

Note: To get a session token you can use the [SGWCLI](https://github.com/mminichino/sgwcli):
```
./sgwcli auth session -h 127.0.0.1 -n database -U sgw@user --ssl
```

```blipctl``` arguments:

| Options                                   | Description                      |
|-------------------------------------------|----------------------------------|
| --ssl                                     | Use SSL                          |
| -n HOST, --host HOST                      | Hostname or IP address           |
| -P PORT, --port PORT                      | Port number                      |
| -u USER, --user USER                      | User Name                        |
| -p PASSWORD, --password PASSWORD          | User Password                    |
| -d DATABASE, --database DATABASE          | Sync Gateway Database            |
| -t SESSION, --session SESSION             | Session Token                    |
| -O, --screen                              | Output documents to the terminal |
| -f, --file                                | Output documents to file(s)      |
| -D DIR, --dir DIR                         | Output Directory                 |
| -s SCOPE, --scope SCOPE                   | Scope                            |
| -c COLLECTIONS, --collections COLLECTIONS | Collections                      |
| -vv, --debug                              | Debug output                     | 
| -v, --verbose                             | Verbose output                   | 
