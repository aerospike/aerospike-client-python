
# aerospike.Client.scan

aerospike.Client.scan - scan records in the primary index

## Description

```
status = aerospike.Client.scan ( namespace, set )

```

**aerospike.Client.scan()** will create a scan object, which can be used to scan through
all the records in the primary index.   

## Parameters

The function takes **namespace** (required) and **set** (optional) arguments. The **set** can
be ommitted or *None*.


## Return Values
The return value will be a new *aerospike.Scan* class instance.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

records = []

def callback( (key, meta, bins) ):
    records.append(bins)

scan_obj = self.client.scan(ns, st)

scan_obj.foreach(callback)

print records


```

We expect to see:

```python
# Some sample data present in database
[{'name': u'John', 'numbers': [1, 2, 3, 4, 5]}]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
