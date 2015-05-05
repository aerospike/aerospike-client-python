
# aerospike.Client.admin_revoke_privileges

aerospike.Client.admin_revoke_privileges - revokes privileges from  a user defined role from the Aerospike database

## Description

```
status = aerospike.Client.admin_revoke_privileges ( role, privileges[, policy ] )

```

**aerospike.Client.admin_revoke_privileges()** will revoke *privileges* from a *role* and return the *status* of the operation.

## Parameters

**role**, the role from which the privileges have to be revoked in the Aerospike database.

**privileges**, a list of dictionaries specifying th privileges to be revoked.

**policy**, the dictionary of policy options to be given while revoking privileges from the database.   

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
privileges = [{"code": aerospike.READ}]

status = client.admin_revoke_privileges( role_name, privileges, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
