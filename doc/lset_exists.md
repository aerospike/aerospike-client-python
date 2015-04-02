
# aerospike.Client.lset.exists
Test existence of an object from the lset.

## Description

```
exists = lset.exists(element, policies)
```
**lset.exists()** will check for existence of an object in the lset.    

## Parameters

**element**, the element to check for existence in the lset.   

**policies**, the dictionary of policies to be given while exists.   

## Return Values
Returns true on existence of element in the lset. In case of error, appropriate exceptions will be raised.

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

exists = lset.exists( 1 , policy )

print exists


```

We expect to see:

```python
True
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
