
# aerospike.Client.llist.remove
Remove an object from the llist.

## Description

```
status = llist.remove(element, policies)
```
**llist.remove()** will remove an object from the llist.    

## Parameters

**element**, the element to remove from the llist.   

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

llist = client.llist( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}

status = llist.remove( 1 , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
