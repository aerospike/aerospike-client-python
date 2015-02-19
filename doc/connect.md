
# aerospike.client.connect

## Description

```
client = aerospike.client(config).connect([username[, password]])

```
**config** is a dictionary holding configuration info for the client
```
Dict:
    'hosts': a list of (address, port) tuples identifying the cluster
    'policies': a dictionary of policies
        'timeout'          : default timeout in milliseconds
        'key'              : default key policy for this client
        'exists'           : default exists policy for this client
        'gen'              : default generation policy for this client
        'retry'            : default retry policy for this client
        'consistency_level': default consistency level policy for this client
        'replica'          : default replica policy for this client
        'commit_level'     : default commit level policy for this client
```

**aerospike.client.connect()** will connect the client to the cluster described
by the *hosts* configuration.

##Parameters

**username** and **password** are self explanatory. They are used for
establishing connection to a security enabled Aerospike cluster
(enterprise edition feature). Skip these when connecting to a community
edition cluster.

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike

config = {
  'hosts':    [ ('127.0.0.1', 3000) ],
  'policies': { 'timeout': 1000}}
client = aerospike.client(config).connect()

```

### See Also

- [Client Policies](http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html)

