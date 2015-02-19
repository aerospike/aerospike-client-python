
# aerospike.client.remove_bin

## Description

```
aerospike.client.remove_bin(key, list[, meta[, policy]])
```

**aerospike.client.remove_bin()** will remove a list of bins from a given *record*.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**list** a list of bin names to be removed from the record

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

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)
meta = { 'gen': 5 }
client.remove_bin(key, ['name', 'age'], meta)

```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)

