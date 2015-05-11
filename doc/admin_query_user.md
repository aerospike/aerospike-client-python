
# aerospike.Client.admin_query_user

aerospike.Client.admin_query_user - queries a user in the Aerospike database

## Description

```
user_details = aerospike.Client.admin_query_user ( username[, policy ] )

```

**aerospike.Client.admin_query_user()** will query a *user* with a given *username* and return the *user_details* of the particular user.   

## Parameters

**username**, the username of the user to be queried in the Aerospike database.

**policy**, the dictionary of policy options to be given while querying a user in the database.

## Return Values
Returns a list containing the details of the particular user.

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

user_details = client.admin_query_user( username, policy )

print user_details

```

We expect to see:

```python
['sys-admin', 'read', 'read-write']
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
