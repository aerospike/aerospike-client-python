
# aerospike.Client.admin_query_users

aerospike.Client.admin_query_users - queries all users in the Aerospike database

## Description

```
user_details = aerospike.Client.admin_query_users ( policy )

```

**aerospike.Client.admin_query_users()** will query all users in the Aerospike
database and return the *user_details* of all the users.

## Parameters

**policy**, the dictionary of policy options to be given while querying all users in the database.   

## Return Values
Returns a list containing the details of all users.

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

user_details = client.admin_query_users( policy )

print user_details

```

We expect to see:

```python
[{'roles_size': 1, 'user': 'testsauser', 'roles': ['sys-admin']}, {'roles_size': 1, 'user': 'testuauser', 'roles': ['user-admin']}, {'roles_size': 3, 'user': 'example', 'roles': ['sys-admin', 'read', 'read-write']}]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
