
# aerospike.Client.lmap.get
Get an object from the lmap.

## Description

```
element = lmap.get(key, policies)
```
**lmap.get()** will get an object from the lmap.    

## Parameters

**key**, key of a element to get from the lmap.   

**policies**, the dictionary of policies to be given while get.   

## Return Values
Returns an object from the lmap. In case of error, appropriate exceptions will be raised.

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

element = lmap.get( 'k11' , policy )

print element


```

We expect to see:

```python
{u'k11' : 88}
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
