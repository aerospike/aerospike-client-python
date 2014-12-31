
# aerospike.Client.touch

aerospike.Client.touch - Touch a record in the Aerospike DB

## Description

```
status = aerospike.Client.touch ( key, ttl = 0 [, policies ] )

```

**aerospike.Client.touch()** will touch the given record, resetting its
time-to-live and incrementing its generation.

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

**ttl**, the [time-to-live] in seconds for the record.

**policies**, the dictionary of policies to be given while touch.   

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
    'timeout' : 5000,
    'key' : aerospike.POLICY_KEY_SEND
}

status = client.touch( key, 120, options)

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
