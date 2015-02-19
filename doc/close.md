
# aerospike.client.close


## Description

```
aerospike.client.close()
```

**aerospike.client.close()** will close already opened connection to the Aerospike database.
Returns *None* on successful connection close.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).close()
client.close()

```

