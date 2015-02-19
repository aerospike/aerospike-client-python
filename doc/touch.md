
# aerospike.client.touch


## Description

```
aerospike.client.touch(key, val=0[, meta[, policy]])
```

**aerospike.client.touch()** will touch the given record, resetting its
time-to-live and incrementing its generation.

## Parameters

**key** the tuple (namespace, set, key) representing the key associated with the record

**val**, the [time-to-live](http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl) in seconds for the record.

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

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)
client.touch(key, 120, policy={'timeout': 100})
```

### See Also

- [FAQ](https://www.aerospike.com/docs/guide/FAQ.html)
- [Record TTL and Evictions](https://discuss.aerospike.com/t/records-ttl-and-evictions/737)

