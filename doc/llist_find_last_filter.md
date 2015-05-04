
# aerospike.llist.find_last_filter
Select values from the end of list up to a maximum count after applying
lua filter.
Supported by server versions >= 3.5.8.

## Description

```
status = llist.find_last_filter(count, filter_name, filter_args, policies)
```
**llist.find_last_filter()** will select values from the end of a list up to
maximum count after applying the speicfied lua filter.

## Parameters

**count**, the count of elements to be returned.

**filter_name**, The name of the User-Defined-Function to use as a search filter.

**filter_args**, The list of parameters passed in to the User-Defined-Function filter.

**policies**, the dictionary of policies to be given with find_last_filter.

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
elements = llist.find_last_filter( 2 , 'search_filter', None, policy )

print elements

```

We expect to see:

```python
[80, 90]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
