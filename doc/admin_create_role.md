
# aerospike.Client.admin_create_role

aerospike.Client.admin_create_role - creates a user defined role in the Aerospike database

## Description

```
status = aerospike.Client.admin_create_role ( role, privileges[, policy] )

```

**aerospike.Client.admin_create_role()** will create a *role* , assign the *privileges* to the newly created role and return the *status* of the role creation.   

## Parameters

**role**, the user-defined role name.

**privileges**, a list of dictionaries specifying the privileges to be contained in the newly created role.

**policy**, the dictionary of policy options to be given while creating a role in the database.   

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
role = "example_role"
privileges = [{"code": aerospike.READ, "ns": "test", "set":"demo"}]

status = client.admin_create_role( role, privileges, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
