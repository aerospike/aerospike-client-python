
# aerospike.Client.lstack.push
Push an object onto the lstack.

## Description

```
status = lstack.push(data, policies)
```
**lstack.push()** will push an object onto the lstack.    

## Parameters

**data**, the data that you want to push onto the lstack.   

**policies**, the dictionary of policies to be given while push.   

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
status = lstack.push( 87 , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
