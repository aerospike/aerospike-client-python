
# aerospike.Client.index_remove

aerospike.Client.index_remove - removes an index in the Aerospike database

## Description

```
status = aerospike.Client.index_remove ( policies, ns, index_name )

```

**aerospike.Client.index_remove()** will remove an index with *index_name* in the specified *ns* and returns the *status* of the index deletion.

## Parameters

**policies**, the dictionary of policies to be given while removing the index.   

**ns**, the namespace from which the index is to be removed.

**index_name**, the name of the index to be removed.

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
status = client.index_remove( policy, 'test', 'age_index' )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
