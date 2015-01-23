
# aerospike.Client.remove

aerospike.Client.remove - removes a particular record matching with the given key

## Description

```
status = aerospike.Client.remove ( key, policies )

```

**aerospike.Client.remove()** will remove a particular *record* matching with the *key* from database.
It will return an integer status value or throws an exception in case of an error.   

## Parameters

**key**, the key to identify the reocrd. A tuple with 'ns','set','key' sequentially.   

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )
```

**policies**, the dictionary of policies to be given while removing a record.   
```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }
```

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

policies = { 'timeout' : 0 }

key = ('test', 'demo', 1)

status = client.remove( key, policies )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
