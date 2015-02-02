# aerospike.Client.query

aerospike.Client.query - queries a secondary index on a set in the Aerospike
database

## Description

```
status = aerospike.Client.query ( namespace, set )

```

**aerospike.Client.query()**
will query a *set* with a specified *where* predicate
then invoke a callback function specified in foreach on each record in the result stream.
The bins returned can be filtered by passing bins in the select

## Parameters

The function takes **namespace** (required) and **set** (optional) arguments. The **set** can
be ommitted or *None*.


## Return Values
The return value will be a new *aerospike.Query* class instance.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

records = []

query = self.client.query('test', 'demo')

query.select('name', 'age')

query.where(p.equals('age', 1))
records = []

def print_result((key,metadata,record)):
    records.append(record)

policy = {
    'timeout' : 1000
    }

query.foreach(print_result, policy)

print records


```

We expect to see:

```python
[{'name':'aa', 'age': 99}]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)

