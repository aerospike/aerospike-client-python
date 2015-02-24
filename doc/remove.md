
# aerospike.client.remove

## Description

```
status = aerospike.Client.remove ( key [, meta [, policies ]] )

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
```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )
```

**meta**, the dictionary which will be combined with bins to write a record.
The dictionary contains the generation value.

**policies**, the dictionary of policies to be given while removing a record.   
```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }
```

## Return Values
Returns an integer status. 0(Zero) is success value. In case of error, appropriate exceptions will be raised.

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)
client.remove(key, {'retry': aerospike.POLICY_RETRY_ONCE})

meta = {
	'ttl' : 45
}

status = client.remove( key, meta, policies )

print status


```

### See Also

- [FAQ](https://www.aerospike.com/docs/guide/FAQ.html)
- [Record TTL and Evictions](https://discuss.aerospike.com/t/records-ttl-and-evictions/737)

