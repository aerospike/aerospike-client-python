
# aerospike.Client.lset.add
Add an object to the lset.

## Description

```
status = lset.add(data, policies)
```
**lset.add()** will add an object to the lset.    

## Parameters

**data**, the data to add to the lset.   

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

lset = client.lset( key, ldt_bin, module )

policy = {
    'timeout' : 2000
}
status = lset.add( 87 , policy )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
