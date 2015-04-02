
# aerospike.Client.lset.remove
Remove an object from the lset.

## Description

```
status = lset.remove(element, policies)
```
**lset.remove()** will remove an object from the lset.    

## Parameters

**element**, the element to remove from the lset.   

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

lset = client.lset( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}

status = lset.remove( 1 , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
