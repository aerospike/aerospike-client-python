
# aerospike.client.exists_many

## Description

```
{primary_key: {"gen": <generation>, "ttl": <ttl>}} = aerospike.client.exists_many(keys, policy)
```

**aerospike.client.exists_many()** will batch-read the record metadata
for the given *keys*, and return it as a dict.

## Parameters

**keys**, a list of key tuples.

**policy** optional batch policies. A dictionary with optional fields
- **timeout** read timeout in milliseconds

## Return Values
Returns a dict of records

```
Dict:
    {
        primary_key: {'gen': <generation value>, 'tt': <ttl value>}
    }
    primary_key: the identifier of the record
```

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

keys = [
  ('test', 'demo', 1),
  ('test', 'demo', 2),
  ('test', 'demo', 3),
  ('test', 'demo', 4)
]
records = client.exists_many(keys)
print records
```

We expect to see:

```python
{
  1: {'gen': 2, 'ttl': 2592000},
  2: {'gen': 2, 'ttl': 2592000},
  3: {'gen': 2, 'ttl': 2592000},
  4: {'gen': 2, 'ttl': 2592000}
}

```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)
- [exists()](https://github.com/aerospike/aerospike-client-python/blob/master/doc/exists.md)

