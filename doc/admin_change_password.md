
# aerospike.Client.admin_change_password

aerospike.Client.admin_change_password - changes the password of a particular user in the Aerospike database

## Description

```
status = aerospike.Client.admin_change_password ( policies, username, password )

```

**aerospike.Client.admin_change_password()** will change the *password* of a particular *user* in the aerospike database and return the *status* of the password modification. Only the password of the user whom the client object is pertained to can be changed.

## Parameters

**policies**, the dictionary of policies to be given while changing the password of a user in the database.   

**username**, the username of the user for  whom the password is to be changed in the aerospike database.

**password**, the changed password of the user in the aerospike database.

## Return Values
Returns an integer status code

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect('example', 'example123')
policy = {
            'timeout': 1000
	 }
username = "example"
password = "foo"

status = client.admin_change_password( policy, username, password )

print status

```

We expect to see:

```python
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
