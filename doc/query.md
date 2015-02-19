# aerospike.client.query

## Description

```
aerospike.client.query(namespace[, set])
aerospike.predicates.equals(bin, val)
aerospike.predicates.between(bin, min, max)
aerospike.query.where(_predicate_)
res = aerospike.query.results([policy])
aerospike.query.foreach(callback[, policy])
```

**aerospike.client.query()** will return a Query object to be used for executing
queries over a secondary index of the specified set (which can be ommitted or
`None`).

The Query can be assigned a Predicate object using **aerospike.query.where()**,
then invoked using either **aersopike.query.foreach()** or **aerospike.query.results()**.
**foreach()** invokes a callback function for each of the records streaming back
from the query, while **results** buffers these records and returns them as a
list.

The bins returned can be filtered by using **aerospike.query.select()**.

Predicates currently come in two flavors, **aerospike.predicates.between()** and
**aerospike.predicates.equals()**.

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike
from aerospike import predicates as p
import pprint

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

pp = pprint.PrettyPrinter(indent=2)
query = self.client.query('test', 'demo')
query.select('name', 'age') # matched records return with the values of these bins
# assuming there is a secondary index on the 'age' bin of test.demo
query.where(p.between('age', 20, 30))
names = []
def matched_names((key, metadata, bins)):
    pp.pprint(bins)
    names.append(bins['name'])

query.foreach(matched_names, {'timeout':2000})
pp.pprint(names)
```

### See Also

- [Query](http://www.aerospike.com/docs/guide/query.html)
- [Managing Queries](http://www.aerospike.com/docs/operations/manage/queries/index.html)

