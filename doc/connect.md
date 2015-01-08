
# aerospike.Client.connect

aerospike.Client.connect - establishes a connection to the Aerospike database instance.

## Description

```
client = aerospike.Client(config).connect(<username>, <password>)

```
**config**, is configuration parameter used while creating aerospike.Client instance.
```
Dict: 
    {
        'hosts': [
                ('127.0.0.1', 3000)
                ],
        'policies': {
                'timeout' : 0
                }
    }

```


**aerospike.Client.connect()** will open a new connection to the Aerospike database and returns
aerospike.Clients object.   

##Parameters

**username** and **password** are self explanatory. They are used for establishing connection with security enabled Aerospike database instance.
Skip them while connecting to security disabled Aerospike database instance.

## Return Values
Returns an instance of aeropspike.Client, which can be used later to do usual database operations.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

```

### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
