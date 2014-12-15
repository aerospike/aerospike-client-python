
# aerospike.Client.lstack.peek
Large Stack (lstack) is a large data type optimized for stack-based operations,
such as push and peek. The lstack provides the ability to continously grow
a very large collection of data.

## Description

```
list = lstack.peek(peek_count, policies)
```
**lstack.peek()** will peek top N values from the lstack.    

## Parameters

**peek_count**, the number of values to peek from lstack.   

**policies**, the dictionary of policies to be given while peek.   

## Return Values
Returns a list of peek_count number of elements. In case of error, appropriate exceptions will be raised.

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

list = lstack.peek( 1 , policy )

print list


```

We expect to see:

```python
[87]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
