
# aerospike.Client.isConnected

aerospike.Client.isConnected - checks if a connection to the database exists.

## Description

```
status = aerospike.Client.isConnected()

```

**aerospike.Client.isConnected()** will check if the client is connected to the
database.

## Parameters


## Return Values
The return value will be the status of connection to the database

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
	'hosts': [('127.0.0.1', 3000)]
	}
client = aerospike.client(config).connect()
status = client.isConnected()
print status

```

We expect to see:

```python
# Some sample data present in database
True
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
