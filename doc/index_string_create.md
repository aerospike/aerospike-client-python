
# aerospike.Client.index_string_create

aerospike.Client.index_string_create - creates a string index for a bin in the Aerospike database

## Description

```
status = aerospike.Client.index_string_create ( policies, ns, set, bin, index_name )

```

**aerospike.Client.index_string_create()** will create a string index with *index_name* on the *bin* in the specified *ns*, *set* and returns the *status* of the index creation.

## Parameters

**policies**, the dictionary of policies to be given while creating the index.   

**ns**, the namespace in which the index is to be created.

**set**, the name of the set on which the index is to be created.

**bin**, the name of the bin on which the index is to be created.

**index_name**, the name of the index to be created.

## Return Values
Returns an integer status code.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = {}
status = client.index_string_create( policy, 'test', 'demo',
'name', 'name_index' )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
