
# aerospike.llist.range_limit
Select values from a begin key to an end key of list up to a maximum count
applying a lua filter if specified.
Supported by server versions >= 3.5.8.

## Description

```
status = llist.range_limit(from_val, end_val, count, filter_name, filter_args, policies)
```
**llist.range_limit()** will select values from a begin key to an end key of a list up to
maximum count applying a lua filter if specified.

## Parameters

**from_val**, Value from which to start.

**end_val**, Value at which to stop.

**count**, the count of elements to be returned.

**filter_name**, The name of the User-Defined-Function to use as a search filter.

**filter_args**, The list of parameters passed in to the User-Defined-Function filter.

**policies**, the dictionary of policies to be given with range_limit.

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
elements = llist.range_limit( 10, 40, 2 , None, None, policy )

print elements


```

We expect to see:

```python
[10, 20]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
