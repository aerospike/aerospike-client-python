
# aerospike.client.get

## Description

```
(key, meta, bins) = aerospike.client.get(key, policy)

```

**aerospike.client.get()** will read a *record* with a given *key*, and return
the *record* as a tuple consisting of key, meta and bins.
If the record does not exist the *meta* data will be `None`.
Non-existent bins will appear in the *record* with a `None` value.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**policy** optional read policies. A dictionary with optional fields
- **timeout** read timeout in milliseconds
- **key** one of the [aerospike.POLICY_KEY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec) values
- **consistency_level** one of the [aerospike.POLICY_CONSISTENCY_LEVEL_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga34dbe8d01c941be845145af643f9b5ab) values
- **replica** one of the [aerospike_POLICY_REPLICA_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gabce1fb468ee9cbfe54b7ab834cec79ab) values

## Return Values
Returns a tuple of record components:

```
Tuple:
    (key, meta, bins)
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
  # assuming a record with such a key exists in the cluster
  key = ('test', 'demo', 1)
  (key, meta, bins) = client.get(key)

  print(key)
  print('--------------------------')
  print(meta)
  print('--------------------------')
  print(bins)
except Exception as e:
  print("error: {0}".format(e), file=sys.stderr)
  sys.exit(1)

```

We expect to see:

```
('test', 'demo', None, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
--------------------------
{'gen': 1, 'ttl': 2592000}
--------------------------
{'age': 1, 'name': u'name1'}

```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)

