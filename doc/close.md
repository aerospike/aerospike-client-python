
# aerospike.Client.close

aerospike.Client.close - closes already opened connection to the database.

## Description

```
aerospike.Client.close()

```

**aerospike.Client.close()** will close already opened connection to the Aerospike database.
Returns *None* on successful connection close.


## Return Values
Returns *None*

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).close()

client.close()

```

### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)