
# aerospike.Client.append

aerospike.Client.append - Appends a string to the string value in a bin

## Description

```
status = aerospike.Client.append ( key, bin, value [, policies ] )

```

**aerospike.Client.append()** will append a string *value* to the string value
in a *bin*.

## Parameters

**key**, the key for the record. A tuple with keys
['ns','set','key'] or ['ns','set','digest'].   

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )

```

**bin**, the name of the bin.

**value**, the string to append to the string value in the bin.

**policies**, the dictionary of policies to be given while append.   

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

options = {
    'timeout' : 5000
}

status = client.append( key, bin, value, options)

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
