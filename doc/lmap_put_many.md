
# aerospike.Client.lmap.put_many
Add a map containing the entries to add to the lmap.

## Description

```
status = lmap.put_many(map, policies)
```
**lmap.put_many()** will add a map containing entries to add to the lmap.    

## Parameters

**map**, a map containing entries to add.   

**policies**, the dictionary of policies to be given while put_many.   

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
status = lmap.put_many( [1, 56] , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
