
# aerospike.Client.udf_list

aerospike.Client.udf_list - lists all registered udf's

## Description

```
[ udf_list ] = aerospike.Client.udf_list ( policies )

```

**aerospike.Client.udf_list()** will list down all registered *udf*s present on the
Aerospike database.   

## Parameters

**policies**, the dictionary of policies, which govern the listing of udf's

```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }

```

## Return Values
Returns a list of registered udf's present on the Aerospike database.

```
List:
    [
        {
        'content': <bytearray content of udf>,
        'hash': <bytearray content hash>,
        'name': <lua module name>,
        'type': <udf_type, 0 for lua type>

        }
    ]

```



## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = { 'timeout' : 0 }

udfs_list = client.udf_list( policy )

print udfs_list


```

We expect to see:

```python
[{'content': bytearray(b''),
  'hash': bytearray(b'195e39ceb51c110950bd'),
  'name': 'my_udf1.lua',
  'type': 0},
 {'content': bytearray(b''),
  'hash': bytearray(b'dd79b84bb9a8f7969854'),
  'name': 'aggregate_udf_less_parameter.lua',
  'type': 0},
 {'content': bytearray(b''),
  'hash': bytearray(b'8a2528e8475271877b3b'),
  'name': 'stream_udf.lua',
  'type': 0},
 {'content': bytearray(b''),
  'hash': bytearray(b'362ea79c8b64857701c2'),
  'name': 'aggregate_udf.lua',
  'type': 0},
 {'content': bytearray(b''),
  'hash': bytearray(b'4b88c8c6ecd14ddb7135'),
  'name': 'mymodule.lua',
  'type': 0},
 {'content': bytearray(b''),
  'hash': bytearray(b'635f47081431379baa4b'),
  'name': 'module.lua',
  'type': 0}]

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)