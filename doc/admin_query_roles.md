
# aerospike.Client.admin_query_roles

aerospike.Client.admin_query_roles - queries all user defined roles in the Aerospike database

## Description

```
role_details = aerospike.Client.admin_query_roles ( policy )

```

**aerospike.Client.admin_query_roles()** will query all roles in the Aerospike
database and return the *role_details* of all the roles.

## Parameters

**policy**, the dictionary of policy options to be given while querying all roles in the database.

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
{'example': [{'code': 11, 'ns': 'test', 'set': 'demo'}], 'example_role': [{'code': 11, 'ns': 'test', 'set': 'demo'}], 'read': [{'code': 10, 'ns': '', 'set': ''}], 'read-write': [{'code': 11, 'ns': '', 'set': ''}], 'read-write-udf': [{'code': 12, 'ns': '', 'set': ''}], 'sys-admin': [{'code': 1, 'ns': '', 'set': ''}], 'user-admin': [{'code': 0, 'ns':'', 'set': ''}]}
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
