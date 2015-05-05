
# aerospike.Client.admin_grant_roles

aerospike.Client.admin_grant_roles - grants roles to a user in the Aerospike database

## Description

```
status = aerospike.Client.admin_grant_roles ( username, roles[, policy ] )

```

**aerospike.Client.admin_grant_roles()** will grant *roles* to a *user* with a given *username* and return the *status* of the roles assignment.   

## Parameters

**username**, the username of the user to whom the roles have to be granted in
the Aerospike database.

**roles**, a list specifying the roles to be assigned to the user.

**policy**, the dictionary of policy options to be given while granting roles to a user in the database.   

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
username = "example"
roles = ["read", "read-write", "sys-admin"]

status = client.admin_grant_roles( username, roles, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
