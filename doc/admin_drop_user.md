
# aerospike.Client.admin_drop_user

aerospike.Client.admin_drop_user - drops a user from the Aerospike database

## Description

```
status = aerospike.Client.admin_drop_user ( policies, username )

```

**aerospike.Client.admin_drop_user()** will drop a *user* with a given *username*, and return the *status*

of dropping the user from the database.   

## Parameters

**policies**, the dictionary of policies to be given while dropping a user from the database.   

**username**, the username of the user to be dropped from the Aerospike database.

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

status = client.admin_drop_user( policy, username )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
