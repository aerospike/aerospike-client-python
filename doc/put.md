
# aerospike.client.put

## Description

```python
aerospike.client.put(key, bins[, meta[, policy]])
```

**aerospike.client.put()** will write a record with a given *key* to the cluster

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**bins** a dictionary of bin-name / bin-value pairs

**meta** optional record metadata to be set. A dictionary with fields
- **ttl** the [time-to-live](http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl) in seconds

**policy** optional write policies. A dictionary with optional fields
- **timeout** write timeout in milliseconds
- **key** one of the [aerospike.POLICY_KEY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec) values
- **exists** one of the [aerospike.POLICY_EXISTS_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga50b94613bcf416c9c2691c9831b89238) values
- **gen** one of the [aerospike.POLICY_GEN_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061) values
- **retry** one of the [aerospike.POLICY_RETRY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718) values
- **commit_level** one of the [aerospike.POLICY_COMMIT_LEVEL_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23) values

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike

config = {
  'hosts': [ ('127.0.0.1', 3000) ],
  'timeout': 1500
}
client = aerospike.client(config).connect()
try:
  key = ('test', 'demo', 1)
  bins = {
    'l': [ "qwertyuiop", 1, bytearray("asd;as[d'as;d", "utf-8") ],
    'm': { "key": "asd';q;'1';" },
    'i': 1234,
    's': '!@#@#$QSDAsd;as'
  }
  client.put(key, bins,
             policy={'key': aerospike.POLICY_KEY_SEND},
             meta={'ttl':180})
  # adding a bin
  client.put(key, {'smiley': u"\ud83d\ude04"})

except Exception as e:
  print("error: {0}".format(e), file=sys.stderr)
  sys.exit(1)
```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)

