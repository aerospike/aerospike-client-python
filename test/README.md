Python client Testing
=========

This testing includes unit-testing of all the API's provided by Python client.

Directory Structure
-------------------

```
test--
     |__ test_get.py
     |
     |__ test_kv.py
     |
     |__ run
```

test_get.py :
    It has all the test cases regarding aerospike.client.get() API.

test_kv.py :
    It is a general test suite which includes all API's single test cases.

Dependencies
------------
The test suite uses pytest.

`pip install pytest`

Execution
---------

You will find an executable bash script named _run_, which will kick start execution of all the test cases.
You can execute individual test case by specifying individual test case name as command line parameter to _run_ script.
e.g.
```
$: ./run -v test_get_with_key_digest
```
For more options check [Pytest usage] and run
```
$: py.test -v [options]
```

[Pytest usage]:http://pytest.org/latest/usage.html

To set the server details, modify the file config.conf
If a community-edition server is to be used, specify the list of hosts in the
`[community-edition]` section. You can remove the `[enterprise-edition]` section
or leave its options empty.

Hosts values are `address:port`, with multiple hosts separated by a semi-colon.

```
[community-edition]
hosts: 192.168.0.2:3000;192.168.0.1:3000;
```

Enterprise Edition Testing
--------------------------
Make sure that a test user has the correct
[permissions](http://www.aerospike.com/docs/guide/security.html#permissions) to
execute the tests. For example:

```sql
CREATE USER pytest PASSWORD just-test-me ROLES read-write-udf, sys-admin, user-admin, data-admin
```

Edit the config.conf file:
```
[enterprise-edition]
hosts: 192.168.0.1:3000;
user: pytest
password: just-test-me
```

Clean up after running the tests:
```sql
DROP USER pytest
```
