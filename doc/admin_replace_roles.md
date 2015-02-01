
# aerospike.Client.admin_replace_roles

aerospike.Client.admin_replace_roles - replaces roles of a user in the Aerospike database

## Description

```
status = aerospike.Client.admin_replace_roles ( policies, username, roles, roles_size )

```

**aerospike.Client.admin_replace_roles()** will replace *roles* of a *user* with a given *username* and return the *status* of the roles replacement.   

## Parameters

**policies**, the dictionary of policies to be given while replace roles of a user in the database.

**username**, the username of the user for whom the roles have to be replaced in the aerospike database.

**roles**, a list specifying the roles to be added for the user overriding the current roles.

**roles_size**, the length of the *roles* list.

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
roles = ["read-write", "sys-admin"]

status = client.admin_replace_roles( policy, username, roles, len(roles) )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
