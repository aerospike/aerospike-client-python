# aerospike.client.query

## Description

```
aerospike.client.query(namespace[, set])
aerospike.query.select(bins)
aerospike.predicates.equals(bin, val)
aerospike.predicates.between(bin, min, max)
aerospike.query.where(_predicate_)
aerospike.query.apply(module, function[, arguments])
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
list. Without a predicate the query acts similar to a scan.

The bins returned can be filtered by using **aerospike.query.select()**, which
takes a list of bin names.

Predicates currently come in two flavors, **aerospike.predicates.between()** and
**aerospike.predicates.equals()**. Currently, only one may be used per-query.

Finally, a stream UDF may be applied to the records streaming back from the
query to aggregate the results.

## Examples

## Query with Callback Example

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

### Aggregation Example

Assume we registered the following Lua module **stream_udf.lua** with the
cluster.

```lua
local function having_ge_threshold(bin_having, ge_threshold)
    return function(rec)
        debug("group_count::thresh_filter: %s >  %s ?", tostring(rec[bin_having]), tostring(ge_threshold))
        if rec[bin_having] < ge_threshold then
            return false
        end
        return true
    end
end

local function count(group_by_bin)
  return function(group, rec)
    if rec[group_by_bin] then
      local bin_name = rec[group_by_bin]
      group[bin_name] = (group[bin_name] or 0) + 1
      debug("group_count::count: bin %s has value %s which has the count of %s", tostring(bin_name), tostring(group[bin_name]))
    end
    return group
  end
end

local function add_values(val1, val2)
  return val1 + val2
end

local function reduce_groups(a, b)
  return map.merge(a, b, add_values)
end

function group_count(stream, group_by_bin, bin_having, ge_threshold)
  if bin_having and ge_threshold then
    local myfilter = having_ge_threshold(bin_having, ge_threshold)
    return stream : filter(myfilter) : aggregate(map{}, count(group_by_bin)) : reduce(reduce_groups)
  else
    return stream : aggregate(map{}, count(group_by_bin)) : reduce(reduce_groups)
  end
end
```

Find the first name distribution of users in their twenties using a query
aggregation:

```python
# -*- coding: utf-8 -*-
import aerospike
from aerospike import predicates as p
import pprint

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

pp = pprint.PrettyPrinter(indent=2)
query = self.client.query('test', 'users')
query.where(p.between('age', 20, 29))
query.apply('stream_udf', 'group_count', [ 'first_name' ])
names = query.results()
pp.pprint(names)
```

## See Also

- [Query](http://www.aerospike.com/docs/guide/query.html)
- [Managing Queries](http://www.aerospike.com/docs/operations/manage/queries/index.html)
- [User Defined Functions](http://www.aerospike.com/docs/guide/udf.html)
- [Developing Stream UDFs](http://www.aerospike.com/docs/udf/developing_stream_udfs.html)

