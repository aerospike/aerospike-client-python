
# aerospike.Client.operate

aerospike.Client.operate - Multiple operations on a single record

## Description

```
key, meta, bins = aerospike.Client.operate ( key, operations, policies )

```

**aerospike.Client.operate()** allows for multiple per-bin operations on
a *record* with a given *key*, with write operations happening before read ones.
Non-existent bins being read will have a NULL value.

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

**opeartions**, an array of one or more per-bin operations conforming
to the following structure.

**policies**, the dictionary of policies to be given while append.   

## Return Values
Returns a tuple of record having key, meta and bins sequentially.

```
Tuple:
    ( key, meta, bins )
    key   : a tuple containing (ns, set, primary_index_key, key_digest)

    meta  : a dict containing { 'gen' : <genration value>, 'ttl': <ttl value>}

    bins  : a dict containing bins retrieved by read operations


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

list = [
        {
            "op" : aerospike.OPERATOR_APPEND,
            "bin" : "name",
            "val" : "aa"
        },
        {
            "op" : aerospike.OPERATOR_READ,
            "bin" : "name"
        }
    ]
        
options = {
    'timeout' : 5000,
    'key' : aerospike.POLICY_KEY_SEND
}

key, meta, bins = self.client.operate(key, list, options)

print key
print meta
print bins


```

We expect to see:

```python
('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
{'gen': 1, 'ttl': 2592000}
{'name': u'aa'}

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
