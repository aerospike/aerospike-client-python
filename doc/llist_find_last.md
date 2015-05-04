
# aerospike.llist.find_last
Select values from the end of list up to a maximum count.
Supported by server versions >= 3.5.8.

## Description

```
status = llist.find_last(count, policies)
```
**llist.find_last()** will select values from the end of a list up to
maximum count.    

## Parameters

**count**, the count of elements to be returned.

**policies**, the dictionary of policies to be given with find_last.   

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
elements = llist.find_last( 2 , policy )

print elements


```

We expect to see:

```python
[70, 80]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
