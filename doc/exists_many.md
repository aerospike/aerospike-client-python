
# aerospike.Client.exists_many

aerospike.Client.exists_many - read the meta-data of records only from the database in a batch

## Description

```
{ primary_key : { "gen" : <generation>, "ttl" : <ttl> } } = aerospike.Client.exists_many ( keys, policies )

```

**aerospike.Client.exists_many()** will read *records* meta data only matching with given *keys*, and return 
as a dict with key as a primary_key and value as dict with meta-data containing *gen* and *ttl*.   

## Parameters

**keys**, the list / tuple of keys. An individual key is a tuple with 'ns','set','key' sequentially.   

```
List:
    keys = [
            ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> ),....
            ]

```

**policies**, the dictionary of policies to be given while reading a record.   

## Return Values
Returns a dict of record with key to be a primary key and value to be a record.

```
Dict:
    {
        primary_key : {
        "gen" : <generation value>,
        "ttl" : <ttl value>
        }
    }

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

keys = [ 
        ('test', 'demo', 1),
        ('test', 'demo', 2),
        ('test', 'demo', 3),
        ('test', 'demo', 4)
    ]

records = client.exists_many( keys )

print records

```

We expect to see:

```python
{
    1: {'gen': 2, 'ttl': 2592000}, 
    2: {'gen': 2, 'ttl': 2592000}, 
    3: {'gen': 2, 'ttl': 2592000}, 
    4: {'gen': 2, 'ttl': 2592000}
}

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
