
# aerospike.Client.llist.get
Get an object from the llist.

## Description

```
element = llist.get(element, policies)
```
**llist.get()** will get an object from the llist.    

## Parameters

**element**, the element to get from llist.   

**policies**, the dictionary of policies to be given while get.   

## Return Values
Returns an object from the llist. In case of error, appropriate exceptions will be raised.

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

element = llist.get( 1 , policy )

print element


```

We expect to see:

```python
1
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
