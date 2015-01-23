
# aerospike.Client.admin_set_password

aerospike.Client.admin_set_password - sets the password of a particular user in the Aerospike database

## Description

```
status = aerospike.Client.admin_set_password ( policies, username, password )

```

**aerospike.Client.admin_set_password()** will set the *password* of a particular *user* in the aerospike database and return the *status* of the result.

## Parameters

**policies**, the dictionary of policies to be given while setting the password of a user in the database.   

**username**, the username of the user for whom the password is to be set in the aerospike database.

**password**, the password of the user to be set in the aerospike database.

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

status = client.admin_set_password( policy, username, password )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
