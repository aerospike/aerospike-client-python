
# aerospike.Client.lstack.size
Large Stack (lstack) is a large data type optimized for stack-based operations,
such as push and peek. The lstack provides the ability to continously grow
a very large collection of data.

## Description

```
size = lstack.size(policies)
```
**lstack.size()** will get the current item count of the lstack.    

## Parameters

**policies**, the dictionary of policies to be given while size.   

## Return Values
Returns the current item count of the lstack. In case of error, appropriate exceptions will be raised.

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

size = lstack.size(policy)

print size


```

We expect to see:

```python
3
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
