
# aerospike.Client.lmap.put
Add an object to the lmap.

## Description

```
status = lmap.put(key, value, policies)
```
**lmap.put()** will add an object(key, value) pair to the lmap.    

## Parameters

**data**, the data to add to the lmap.   

**policies**, the dictionary of poliies to be given while add.   

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

lmap = client.lmap( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}
status = lmap.put( 'k1', 8, policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
