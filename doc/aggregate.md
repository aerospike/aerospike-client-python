
# aggregate

aggregate - applies a stream UDF to the results of a secondary index query.

## Description

```
query = aerospike.Client.query ( namespace, set )
query.select(bins)
query.where(predicate)
query.apply(udf_module, function)

records = []
def user_callback(value):
    records.append(value)

query.foreach(user_callback)

```

The above sequence of instructions will apply a stream UDF function to the result of running a secondary index query on the given **namespace** and **set**.

## Parameters

The function takes **namespace** (required) and **set** (optional) arguments. The **set** can
be ommitted or *None*.

**bins** is a comma separated names of bins on which the query should be applied.

**predicate** specifies particular values of the bins on which the query should be run.

**udf_module**, specifies a registered udf module in the aerospike database.

**function**, specifies a lua function within the module.

## Return Values
The result will be the callback being called for the set of returned records.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

query = client.query('test', 'demo')
query.select('name', 'age')
query.where(p.between('age', 1, 5))
query.apply('stream_example', 'count')

records = []
def user_callback(value):
    records.append(value)

query.foreach(user_callback)

print records[0]

```

We expect to see:

```python
# Some sample data present in database
5
```

### See Also

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
- [Key-Value Store](http://www.aerospike.com/docs/guide/kvs.html)
