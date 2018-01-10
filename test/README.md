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
If testing using TLS, the tls-name of the server can be provided in the hosts entry after the port separated by a '|':
```
[enterprise-edition]
hosts: 192.168.0.1:3000|server-tls-name;
user: pytest
password: just-test-me
```
In order to provide the correct TLS configuration information to the test suite, it will be necessary to edit the config.conf file, under the [tls] section

For example:
```
[tls]
cafile: /path/to/cert.pem
keyfile: /path/to/key.pem
certfile: /path/to/cert.pem
enable: True
```

The following entries are used if provided:

* enable (Whether or not to use TLS for the tests. True)
* cafile Path to a trusted CA certificate file. By default TLS will use system standard trusted CA certificates
* capath Path to a directory of trusted certificates. See the OpenSSL SSL_CTX_load_verify_locations manual page for more information about the format of the directory.
* certfile Path to the client's certificate chain file for mutual authentication. By default mutual authentication is disabled.
* keyfile Path to the client's key for mutual authentication. By default mutual authentication is disabled.
* protocols Specifies enabled protocols. This format is the same as Apache's SSLProtocol documented at https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol . If not specified the client will use "-all +TLSv1.2".
* cipher_suite Specifies enabled cipher suites. The format is the same as OpenSSL's Cipher List Format documented at https://www.openssl.org/docs/manmaster/apps/ciphers.html
* crl_check (True to enable) Enable CRL checking for the certificate chain leaf certificate. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
* crl_check_all (True to enable) Enable CRL checking for the entire certificate chain. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
* log_session_info (True to enable)
* max_socket_idle

Clean up after running the tests:
```sql
DROP USER pytest
```
