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
Execution
---------

You will find an executable bash script named _run_, which will kick start execution of all the test cases.  
You can execute individual test case by specifying individual test case name as command line parameter to _run_ script.  
e.g.
```
$: run test_get_with_key_digest
```
For more options check [Pytest usage] and run
```
$: py.test -v [options]
```

[Pytest usage]:http://pytest.org/latest/usage.html
