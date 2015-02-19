
# aerospike.client.remove

## Description

```
aerospike.client.remove(key, policy)
```

**aerospike.client.remove()** will remove a particular *record* matching the *key* from the cluster.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**policy** optional remove policies. A dictionary with optional fields
- **timeout** in milliseconds
- **key** one of the [aerospike.POLICY_KEY_*](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec) values
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
client.remove(key, {'retry': aerospike.POLICY_RETRY_ONCE})

```

### See Also

- [FAQ](https://www.aerospike.com/docs/guide/FAQ.html)
- [Record TTL and Evictions](https://discuss.aerospike.com/t/records-ttl-and-evictions/737)

