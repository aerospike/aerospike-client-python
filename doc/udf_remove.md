
# aerospike.Client.udf_remove

aerospike.Client.udf_remove - unregisters already registered udf module from the Aerospike database

## Description

```
status = aerospike.Client.udf_remove ( policies, module )

```

**aerospike.Client.udf_remove()** will unregister / remove already registered udf module from the Aerospike database. You can specify filename as a module.

## Parameters

**policies**, the dictionary of policies, which govern the listing of udf's

```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }

```
**module**, udf module name. You can specify filename in place of module name.

## Return Values
Returns an integer status or throws exception in case of errors.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = { 'timeout' : 0 }

status = client.udf_remove( policy, "sample.lua" )

print status



```

We expect to see:

```python
0

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)