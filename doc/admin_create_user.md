
# aerospike.Client.admin_create_user

aerospike.Client.admin_create_user - creates a user in the Aerospike database

## Description

```
status = aerospike.Client.admin_create_user ( username, password, roles[, policies] )

```

**aerospike.Client.admin_create_user()** will create a *user* with a given *username* and *password*, assign the *roles* to the newly created user and return the *status* of the user creation.   

## Parameters

**username**, the username of the user to be added to the Aerospike database.

**password**, the password of the user to be added to the Aerospike database.

**roles**, a list specifying the roles to be assigned to the newly created user.

**policies**, the dictionary of policies to be given while creating a user in the database.   

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
password = "foo"
roles = ["read", "read-write", "sys-admin"]

status = client.admin_create_user( username, password, roles, policy )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
