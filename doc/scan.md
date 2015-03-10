
# aerospike.client.scan

## Description

```
aerospike.client.scan(namespace[, set])
res = aerospike.scan.results([policy])
aerospike.scan.foreach(callback[, policy, options])
```

**aerospike.client.scan()** will return a Scan object to be used for executing
scans over a specified set (which can be ommitted or `None`).

The Scan can be invoked using either **aersopike.scan.foreach()** or
**aerospike.scan.results()**. **foreach()** invokes a callback function for
each of the records streaming back from the scan, while **results** buffers
these records and returns them as a list.

The bins returned can be filtered by using **aerospike.scan.select()**.

## Examples

```python
# -*- coding: utf-8 -*-
import aerospike
import pprint

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

pp = pprint.PrettyPrinter(indent=2)
scan = aerospike.client.scan('test', 'demo')
scan.select('name', 'age') # matched records return with the values of these bins
names = []
def collect_names((key, metadata, bins)):
    pp.pprint(bins)
    names.append(bins['name'])

select.foreach(collect_names, {'timeout':2000}, {'percent': 100, 'concurrent': True})
pp.pprint(names)
```

### See Also

- [Scans](http://www.aerospike.com/docs/guide/scan.html)
- [Managing Scans](http://www.aerospike.com/docs/operations/manage/scans/)

