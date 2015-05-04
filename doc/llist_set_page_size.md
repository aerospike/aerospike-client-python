
# aerospike.llist.set_page_size
Sets the page size of the ldt bin.
Supported by server versions >= 3.5.8.

## Description

```
status = llist.set_page_size(page_size, policies)
```
**llist.set_page_size()** sets the page size of the ldt bin.

## Parameters

**page_size**, the page size the ldt bin should be set to.

**policies**, the dictionary of policies to be given with set_page_size().

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
elements = llist.set_page_size( 8192 , policy )

print elements


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
