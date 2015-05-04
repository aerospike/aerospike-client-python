
# aerospike.llist.find_first
Select values from the beginning of list up to a maximum count.
Supported by server versions >= 3.5.8.

## Description

```
status = llist.find_first(count, policies)
```
**llist.find_first()** will select values from the beginning of a list up to
maximum count.    

## Parameters

**count**, the count of elements to be returned.

**policies**, the dictionary of policies to be given with find_first.   

## Return Values
Returns a list of ldt-bin entries. In case of error, appropriate exceptions will be raised.

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
elements = llist.find_first( 2 , policy )

print elements


```

We expect to see:

```python
[10, 20]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
