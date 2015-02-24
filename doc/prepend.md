
# aerospike.client.prepend

## Description

```
aerospike.client.prepend ( key, bin, val [, meta [, policies ]] )

```

**aerospike.client.prepend()** will prepend a string *value* to the string value
in a *bin*.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**bin** the name of the bin.

**val**, the string to prepend to the string value in the bin.

**meta** optional record metadata to be set. A dictionary with fields
- **ttl** the [time-to-live](http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl) in seconds

**policies**, the dictionary of policies to be given while prepend.   

**policy** optional write policies. A dictionary with optional fields
- **timeout** write timeout in milliseconds
- **key** one of the [aerospike.POLICY_KEY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec) values
- **gen** one of the [aerospike.POLICY_GEN_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061) values
- **retry** one of the [aerospike.POLICY_RETRY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718) values
- **commit_level** one of the [aerospike.POLICY_COMMIT_LEVEL_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23) values
- **consistency_level** one of the [aerospike.POLICY_CONSISTENCY_LEVEL_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga34dbe8d01c941be845145af643f9b5ab) values
- **replica** one of the [aerospike_POLICY_REPLICA_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gabce1fb468ee9cbfe54b7ab834cec79ab) values

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike


config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

try:
  key = ('test', 'demo', 1)
  meta = {
	  'ttl' : 88
  }
  client.prepend(key, 'name', 'Dr. ', meta, policy={'timeout': 1200})
except Exception as e:
  print("error: {0}".format(e), file=sys.stderr)
  sys.exit(1)
```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)

