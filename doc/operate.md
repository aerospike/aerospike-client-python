
# aerospike.client.operate

## Description

```
(key, meta, bins) = aerospike.client.operate(key, list[, meta[, policy]])
```

**aerospike.client.operate()** allows for multiple bin operations on
a *record* with a given *key*, with write operations happening before read ones.
Non-existent bins being read will have a `None` value.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**list** a list of one or more bin operations, each structured as the dictionary 
{'bin': bin name, 'op': aerospike.OPERATOR\_\*\[, 'val': value\]}

**meta** optional record metadata to be set. A dictionary with fields
- **ttl** the [time-to-live](http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl) in seconds

**policy** optional write policies. A dictionary with optional fields
- **timeout** write timeout in milliseconds
- **key** one of the [aerospike.POLICY_KEY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec) values
- **gen** one of the [aerospike.POLICY_GEN_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061) values
- **retry** one of the [aerospike.POLICY_RETRY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718) values
- **commit_level** one of the [aerospike.POLICY_COMMIT_LEVEL_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23) values
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
  key = ('test', 'demo', 1)
  client.put(key, {'count': 1})
  list = [
    {
      "op" : aerospike.OPERATOR_INCR,
      "bin": "count",
      "val": 2,
      "initial_value": 0
    },
    {
      "op" : aerospike.OPERATOR_PREPEND,
      "bin": "name",
      "val": ":start:"
    },
    {
      "op" : aerospike.OPERATOR_APPEND,
      "bin": "name",
      "val": ":end:"
    },
    {
      "op" : aerospike.OPERATOR_READ,
      "bin": "name"
    },
    {
      "op" : aerospike.OPERATOR_WRITE,
      "bin": "age",
      "val": 39
    },
    {
      "op" : aerospike.OPERATOR_TOUCH,
      "val": 360
    }
  ]
  meta = {
	  'ttl' : 5865
  }
  (key, meta, bins) = self.client.operate(key, list, meta, policy={'timeout':500})

  print(key)
  print('--------------------------')
  print(meta)
  print('--------------------------')
  print(bins)
except Exception as e:
  print("error: {0}".format(e), file=sys.stderr)
  sys.exit(1)

```


### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)

