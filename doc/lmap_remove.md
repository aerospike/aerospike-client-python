
# aerospike.Client.lmap.remove
Remove an object from the lmap.

## Description

```
status = lmap.remove(key, policies)
```
**lmap.remove()** will remove key-value pair from the lmap.    

## Parameters

**key**, key of element to remove from the lmap.   

**policies**, the dictionary of policies to be given while remove.   

## Return Values
Returns 0 on success. In case of error, appropriate exceptions will be raised.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)

lmap = client.lmap( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}

status = lmap.remove( 'k1' , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
