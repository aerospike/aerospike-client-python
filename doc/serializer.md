
# aerospike.set_serializer

aerospike.set_serializer - sets a serializer in the Aerospike database

## Description

```
status = aerospike.set_serializer ( func )

```

**aerospike.set_serializer()** will set a serializer with a given *func*, and return the *status*

## Parameters

**func**, the function to be called when data needs to be serialized.

## Return Values
Returns the status of setting the serializer.

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

def serialize_function(val):
   return pickle.dumps(val)

status = aerospike.set_serializer( serialize_function )

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
