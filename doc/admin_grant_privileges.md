
# aerospike.Client.admin_grant_privileges

aerospike.Client.admin_grant_privileges - grants privileges to a role in the Aerospike database

## Description

```
status = aerospike.Client.admin_grant_privileges ( role, privileges[, policy ] )

```

**aerospike.Client.admin_grant_privileges()** will grant *privileges* to a *role* and return the *status* of the privileges assignment.   

## Parameters

**role**, the user defined role to which the privileges have to be granted in the Aerospike database.

**privileges**, a list of dictionaries specifying the privileges to be assigned to the role.

**policy**, the dictionary of policy options to be given while granting privileges to a role in the database.   

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
privileges = [{"code": aerospike.READ}]

status = client.admin_grant_privileges( role, privileges, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
