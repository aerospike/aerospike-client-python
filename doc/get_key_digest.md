
# aerospike.Client.get_key_digest

aerospike.Client.get_key_digest - calculates the digest of a particular key.

## Description

```
digest = aerospike.Client.get_key_digest ( ns, set, key )

```

**aerospike.Client.get_key_digest()** gets the digest for a particular *ns*, *set* and *key*.

## Parameters
**ns**, the namespace in the aerospike database.

**set**, the set in the aerospike database.
 
**key**, the key for which the digest is to be returned.

## Return Values
Returns the digest of a particular key.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

digest = client.get_key_digest("test", "demo", 1 )

print digest

```

We expect to see:

```python
���8���g�h>�����F
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
