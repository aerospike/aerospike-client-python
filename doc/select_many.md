
# aerospike.client.select_many

## Description

```
{primary_key: (key, meta, bins)} = aerospike.client.select_many (keys, bins, policy)
```

**aerospike.client.select_many()** will batch-read the given *keys*, and return
the *records* with filtered *bins* as a dict of _key_: (key, meta, bins).

## Parameters

**keys**, a list of key tuples.

**policy** optional read policies. A dictionary with optional fields
- **timeout** read timeout in milliseconds

## Return Values
Returns a dict of records

```
Dict:
    {
        primary_key: (key, meta, bins)
    }
    primary_key: the identifier of the record
    key : a tuple (namespace, set, primary key, the record's RIPEMD-160 digest)
    meta: a dict containing { 'gen' : <genration value>, 'ttl': <ttl value>}
    bins: a dict containing bin-name/bin-value pairs
```

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

try:
  keys = [
    ('test', 'demo', 1),
    ('test', 'demo', 2),
    ('test', 'demo', 3),
    ('test', 'demo', 4),
    ('test', 'demo', 5),
  ]
  bins = [ 'age', u'name' ]
  records = client.select_many( keys, bins, {'timeout': 1000} )
  print records
except Exception as e:
  print("error: {0}".format(e, file=sys.stderr))
  sys.exit(1)
```

We expect to see:

```python
{
  1: (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 1, 'ttl': 2592000}, {'age': 1, 'name': u'Name1'}), 
  2: (('test', 'demo', 2, bytearray(b'\xaejQ_7\xdeJ\xda\xccD\x96\xe2\xda\x1f\xea\x84\x8c:\x92p')), {'gen': 1, 'ttl': 2592000}, {'age': 2, 'name': u'Name2'}), 
  3: (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 1, 'ttl': 2592000}, {'age': 3, 'name': u'Name3'}), 
  4: (('test', 'demo', 4, bytearray(b'\x15\x1d\x15\xfa\x00a\xb3\x8f\xb5_\xf2>e\xae:\xeeS\x16\x93%')), {'gen': 1, 'ttl': 2592000}, {'age': 4, 'name': u'Name4'}),
  5: None
}

```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)
- [select()](https://github.com/aerospike/aerospike-client-python/blob/master/doc/select.md)

