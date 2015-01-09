
# aerospike.Client.select

aerospike.Client.select - project bins on given namespace and set

## Description

```
(key, meta, records) = aerospike.Client.select ( key, bins, policies )

```

**aerospike.Client.select()** will select given bins present in the given primary index.   

## Parameters

The function takes **key** (required), **bins** (required) and **policies** (required). 

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )

List:
	bins = [ <bin name String> ]

Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }
```


## Return Values
The function returns a tuple containing, key, meta and records / bins.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)

key, meta, records = client.select( key, ['numbers'], {} )

print records

```

We expect to see:

```python
# Some sample data present in database
{'numbers': [1, 2, 3, 4, 5]}
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
