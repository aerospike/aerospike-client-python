
# aerospike.Client.admin_drop_role

aerospike.Client.admin_drop_role - drops a user defined role from the Aerospike database

## Description

```
status = aerospike.Client.admin_drop_role ( role[, policies ] )

```

**aerospike.Client.admin_drop_role()** will drop a *role* and return the *status* of dropping the role from the database.   

## Parameters

**role**, the role to be dropped from the Aerospike database.

**policies**, the dictionary of policies to be given while dropping a user from the database.   

## Return Values
Returns an integer status code


## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect('admin', 'admin')

policy = {
	    'timeout': 1000
	}
role_name = "example_role"

status = client.admin_drop_role( role_name, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
