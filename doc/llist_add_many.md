
# aerospike.Client.llist.add_many
Add a list of objects to the llist.

## Description

```
status = llist.add_many(list, policies)
```
**llist.add_many()** will add a list of objects to the llist.    

## Parameters

**list**, the list of objects.   

**policies**, the dictionary of policies to be given while add_many.   

## Return Values
Returns an integer status. 0(Zero) is success value. In case of error, appropriate exceptions will be raised.

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
status = llist.add_many( [1, 56] , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
