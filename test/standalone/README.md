These tests are designed to run manually. The automated tests in `new_tests` are designed to pass on any server
configuration, but there are certain features that rely on certain server behavior or configurations out of
the control of those automated tests.

## How to run tests

### `test_node_close_listener.py`

Requires Docker configured to use the former.

Reason for test: `new_tests` does not manipulate the server.

### `test_cluster_name.py`

Only requires Docker.

Reason for test: The client in `new_tests` does not expect a cluster name. Also, the cluster name can change depending
on the server configuration.

### `test_timeout_delay.py`

Requires a server configured to add latency to the `get()` command. We don't have time to automate this, for now.

Also requires pytest so we can parametrize the test cases more easily.

#### Alternate methods to test (that failed to work)

We tried using `tcconfig` to introduce latency in the network between the client and server, but this causes the tend thread to hang because it needs to refresh the server's nodes on each tend. The connection recovery and abort process for timeout delay
happens in the tend thread, so the tend thread needs to run uninterrupted.

With this method, we used a specific server config where `keepalive-enabled` is `false`, so that the server doesn't
close the client connection. `proto-fd-idle-ms` seems to be the right server option, but it was deprecated in favor of
`keep-alive`: https://aerospike.com/docs/database/reference/config/#service__proto-fd-idle-ms
