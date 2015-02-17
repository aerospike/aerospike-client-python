
# aerospike.Client.udf_getRegistered

aerospike.Client.udf_getRegistered - gets the code for a UDF module registered with the
server

## Description

```
udf_content = aerospike.Client.udf_getRegistered ( module [, language, policies ] )

```

**aerospike.Client.udf_getRegistered()** will populates code with the content
of the matching UDF *module* that was previously registered with the server.

## Parameters

**module**, the name of the UDF module to get from the server.

**language**, one of Aerospike::UDF_TYPE_*.

**policies**, the dictionary of policies to be given while udf_getRegistered. 

**[policies](aerospike.md)** including,  
- **timeout**

## Return Values
Returns content of the UDF module.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

module = "bin_lua.lua"
language = aerospike.UDF_TYPE_LUA
policy = {
    'timeout' : 5000
}

udf_content = client.udf_getRegistered( module, language, policy)

print udf_content


```

We expect to see:

```python

function mytransform(rec, bin, offset)
    rec['age'] = rec['age'] + offset
aerospike:update(rec)
    end
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
