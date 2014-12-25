
# aerospike.Client.get

aerospike.Client.get - gets a record from the Aerospike database

## Description

```
(key, meta, bins) = aerospike.Client.get ( key, policies )

```

**aerospike.Client.get()** will read a *record* with a given *key*, and return the *record*

as a tuple consisting of key, meta and bins.   

Non-existent bins will appear in the *record* with a None value.  

## Parameters

**key**, the key under which to store the record. A tuple with 'ns','set','key' sequentially.   

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )

```

**policies**, the dictionary of policies to be given while reading a record.   

## Return Values
Returns a tuple of record having key, meta and bins sequentially.

```
Tuple:
    ( key, meta, bins )
    key   : a tuple containing (ns, set, primary_index_key, key_digest)

    meta  : a dict containing { 'gen' : <genration value>, 'ttl': <ttl value>}

    bins  : a dict containing bin name as key and value


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

(key, meta, bins) = client.get( key )

print key
print meta
print bins



```

We expect to see:

```python
('test', 'demo', None, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
{'gen': 1, 'ttl': 2592000}
{'age': 1, 'name': u'name1'}

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)