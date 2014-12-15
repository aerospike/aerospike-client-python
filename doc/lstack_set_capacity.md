
# aerospike.Client.lstack.set_capacity
Large Stack (lstack) is a large data type optimized for stack-based operations,
such as push and peek. The lstack provides the ability to continously grow
a very large collection of data.

## Description

```
status = lstack.set_capacity(capacity, policies)
```
**lstack.set_capacity()** will set the max capacity for the lstack.    

## Parameters

**capacity**, the max capacity for the lstack.   

**policies**, the dictionary of policies to be given while peek.   

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

lstack = client.lstack( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}

status = lstack.set_capacity(25, policy)

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
