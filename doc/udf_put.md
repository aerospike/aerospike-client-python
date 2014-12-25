
# aerospike.Client.udf_put

aerospike.Client.udf_put - puts udf module to the Aerospike database.

## Description

```
status = aerospike.Client.udf_put ( policies, filename, udf_type )

```

**aerospike.Client.udf_put()** will put udf module specified to the 
Aerospike database.   

## Parameters

**policies**, the dictionary of policies, which govern the listing of udf's

```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }

```
**filename**, the name of the udf module, which will be registered with the Aerospike database.

**udf_type**, an integer value to specify type of udf module. **0** for lua module.

## Return Values
Returns an integer status or throws ena exception in case of errors.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = { 'timeout' : 0 }

status = client.udf_put( policy, "sample.lua", 0 )

print status


```

We expect to see:

```python
0

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)