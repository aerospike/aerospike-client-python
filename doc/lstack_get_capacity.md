
# aerospike.Client.lstack.get_capacity
Large Stack (lstack) is a large data type optimized for stack-based operations,
such as push and peek. The lstack provides the ability to continously grow
a very large collection of data.

## Description

```
capacity = lstack.get_capacity(capacity, policies)
```
**lstack.get_capacity()** will get the current capacity limit for the lstack.    

## Parameters

**policies**, the dictionary of policies to be given while peek.   

## Return Values
Returns capacity limit for the lstack. In case of error, appropriate exceptions will be raised.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)

lstack = client.lstack( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}

capacity = lstack.get_capacity(policy)

print capacity


```

We expect to see:

```python
25
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
