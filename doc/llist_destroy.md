
# aerospike.Client.llist.destroy

## Description

```
status = llist.destroy(policies)
```
**llist.destroy()** will delete the entire llist.    

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

llist = client.llist( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}
status = llist.destroy( policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
