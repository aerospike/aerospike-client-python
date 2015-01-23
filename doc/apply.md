
# aerospike.Client.apply

aerospike.Client.apply - applies a registered udf module on a particular record.

## Description

```
status = aerospike.Client.apply ( key, module, function, args, policies )

```

**aerospike.Client.apply()** will apply a registered udf module from the Aerospike database on a particular record.

## Parameters

**key**, the key under which to store the record. A tuple with 'ns','set','key' sequentially.   

```
Tuple:
    key = ( <namespace>, 
            <set name>, 
            <the primary index key>, 
            <a RIPEMD-160 hash of the key, and always present> )

```
**module**, udf module name.

**function**, the udf function within the udf module to be applied on the record.

**args**, the arguments to the **function**. The parameter should be a list of arguments.

**policies**, the dictionary of policies, which govern the application of udf function.

```
Dict:
    policies = {
            'timeout' : <Only integer timeout value>
    }

```

## Return Values
Returns the result of the udf function applied on the record.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

policy = { 'timeout' : 0 }

return_value = client.apply(key, 'sample', 'list_append', ['name', 'car'])

print return_value


```


We expect to see:

```python
0

```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
