
# aerospike.Client.exists

aerospike.Client.exists - checks if a record exists in the Aerospike database

## Description

```
( key, meta ) = aerospike.Client.exists ( key, policies )

```

**aerospike.Client.exists()** will check if a *record* with a given *key* exists in the database, and returns the *record*

as a tuple consisting of key and meta.   

## Parameters

**key**, the key under which the record is stored . A tuple with 'ns','set','key' sequentially.

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )

```

**policies**, the dictionary of policies to be given while checking if a record exists.   

## Return Values
Returns a tuple of record having key and meta sequentially.

```
Tuple:
    ( key, meta )
    key   : a tuple containing (ns, set, primary_index_key, key_digest)

    meta  : a dict containing { 'gen' : <genration value>, 'ttl': <ttl value>}

gen: reflects the number of times the record has been altered

ttl: time in seconds until the record expires

```



## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)

( key, meta ) = client.exists( key )

print key
print meta



```

We expect to see:

```python
('test', 'demo', None, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
{'gen': 1, 'ttl': 2592000}
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
