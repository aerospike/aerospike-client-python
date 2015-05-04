
# aerospike.Client.admin_query_roles

aerospike.Client.admin_query_roles - queries all user defined roles in the Aerospike database

## Description

```
role_details = aerospike.Client.admin_query_roles ( policies )

```

**aerospike.Client.admin_query_roles()** will query all roles in the Aerospike
database and return the *role_details* of all the roles.

## Parameters

**policies**, the dictionary of policies to be given while querying all roles in the database.

## Return Values
Returns a list containing the details of all roles.

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

role_details = client.admin_query_roles( policy )

print role_details

```

We expect to see:

```python
[{'privileges': [{'code': 11, 'ns': 'test', 'set': 'demo'}], 'privileges_size': 1, 'role': 'example'}, {'privileges': [{'code': 11, 'ns': 'test', 'set': 'demo'}], 'privileges_size': 1, 'role': 'example_role'}, {'privileges': [{'code': 10, 'ns': '', 'set': ''}], 'privileges_size': 1, 'role': 'read'}, {'privileges': [{'code': 11, 'ns': '', 'set': ''}], 'privileges_size': 1, 'role': 'read-write'}, {'privileges': [{'code': 12, 'ns': '', 'set': ''}], 'privileges_size': 1, 'role': 'read-write-udf'}, {'privileges': [{'code': 1, 'ns': '', 'set': ''}], 'privileges_size': 1, 'role': 'sys-admin'}, {'privileges': [{'code': 0, 'ns': '', 'set': ''}], 'privileges_size': 1, 'role': 'user-admin'}]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
