
# aerospike.set_deserializer

aerospike.set_deserializer - sets a deserializer in the Aerospike database

## Description

```
status = aerospike.set_deserializer ( func )

```

**aerospike.set_deserializer()** will set a deserializer with a given *func*, and return the *status*

## Parameters

**func**, the function to be called when data needs to be deserialized.

## Return Values
Returns the status of setting the deserializer.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
import cPickle as pickle

config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

key = ('test', 'demo', 1)

def deserialize_function(val):
   return pickle.loads(val)

status = aerospike.set_deserializer( deserialize_function )

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
