
# aerospike.Client.put

aerospike.Client.put - puts a record to the Aerospike database

## Description

```
status = aerospike.Client.put ( key, bins, meta, policies )

```

**aerospike.Client.put()** will write a *record* with a given *key*, and return the *status*

as an integer value.   

## Parameters

**key**, the key under which to store the record. A tuple with 'ns','set','key' sequentially.   

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )
Dict:
    bins = {
        "bin-name" : "bin-value"
    }
```
**meta**, the dictionary which will be combined with bins to write a record.

**policies**, the dictionary of policies to be given while reading a record.   

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

rec = {
        'i': [ "nanslkdl", 1, bytearray("asd;as[d'as;d", "utf-8") ],
        's': { "key": "asd';q;'1';" },
        'b': 1234,
        'l': '!@#@#$QSDAsd;as'
    }

status = client.put( key, rec )

print status


```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)