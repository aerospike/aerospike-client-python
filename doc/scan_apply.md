
# aerospike.Client.scan_apply

aerospike.Client.scan_apply -  Apply a record UDF to each record in 
a background scan.

## Description

```
scan_id = aerospike.Client.scan_apply ( ns, set, module, function,
        args [, policies]  )

```

**aerospike.Client.scan_apply** will initiate a background read/write
scan and apply a record UDF *module.function* with *args* to each record being
scanned in *ns.set*.

## Parameters

**ns**, the namespace

**set**, the set to be scanned

**module**, the name of the UDF module registered against the Aerospike DB.

**function**, the name of the function to be applied to the records.

**args**, an array of arguments for the UDF.

**policies**, the dictionary of policies to be given while scan_apply.  

**[policies](aerospike.md)** including,
- **timeout**

**options**, the dictionary of scan options that are set while
executing scan.  

**[options](aerospike.md)** including,   
- **priority**
- **percent**
- **no_bins**
- **concurrent**

## Return Values
Returns scan_id an integer handle for the initiated background scan.

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

print scan_id


```

We expect to see:

```python
4562397
    end
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
