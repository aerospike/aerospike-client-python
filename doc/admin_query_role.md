
# aerospike.Client.admin_query_role

aerospike.Client.admin_query_role - queries a role in the Aerospike database

## Description

```
role_details = aerospike.Client.admin_query_role ( role[, policies ] )

```

**aerospike.Client.admin_query_role()** will query a *role* and return the *role_details* for the particular role

## Parameters

**role**, the user defined role to be queried in the Aerospike database.

**policies**, the dictionary of policies to be given while querying a role in the database.

## Return Values
Returns a list containing the details of the particular role.

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

role_details = client.admin_query_role( role_name, policy )

print role_details

```

We expect to see:

```python
[{'privileges': [{'code': 11, 'ns': 'test', 'set': 'demo'}], 'privileges_size': 1, 'role': 'example_role'}]
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
