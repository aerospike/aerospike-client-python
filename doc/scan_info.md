
# aerospike.Client.scan_info

aerospike.Client.scan_info -  Check the status of a background scan.

## Description

```
scan_info = aerospike.Client.scan_info ( scan_id, policy  )

```

**aerospike.Client.scan_info** will return the status of a background read/write
scan referenced by the particular *scan_id* and returns a dictionary containing
the status.

## Parameters

**scan_id**, the id returned by the initiation of the background scan.

**policies**, the dictionary of policies to be given while querying the
particular background scan.

**[policies](aerospike.md)** including,
- **timeout**

## Return Values
Returns scan_info a dictionary containing the details of the scan.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = {
    'timeout' : 5000
}

scan_id = client.scan_apply( 'test', 'demo', 'bin_lua', 'mytransform',
        ['age', 2], policy)

scan_info = client.scan_info(scan_id, policy)

print scan_info


```

We expect to see:

```python
{'status': 3L, 'records_scanned': 22L, 'progress_pct': 100L}
    end
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
