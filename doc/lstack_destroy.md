
# aerospike.Client.lstack.destroy
Large Stack (lstack) is a large data type optimized for stack-based operations,
such as push and peek. The lstack provides the ability to continously grow
a very large collection of data.

## Description

```
status = lstack.destroy(policies)
```
**lstack.destroy()** will delete the entire stack.    

## Parameters

**policies**, the dictionary of policies to be given while destroy.   

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

lstack = client.lstack( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}
status = lstack.destroy( policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
