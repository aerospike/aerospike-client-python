These tests are designed to run manually. The automated tests in `new_tests` are designed to pass on any server
configuration, but there are certain features that rely on certain server behavior or configurations out of
the control of those automated tests.

## How to run tests

- `test_node_close_listener.py` requires Docker configured to use the former.

Reason for test: `new_tests` does not manipulate the server.

- `test_cluster_name.py` only requires Docker.

Reason for test: The client in `new_tests` does not expect a cluster name. Also, the cluster name can change depending
on the server configuration.

- `test_timeout_delay.py` requires `tcconfig` to introduce latency between the client and server. It also requires Docker.

Reason for test: we need a specific server config where `keepalive-enabled` is `false`, so that the server doesn't
close the client connection. `proto-fd-idle-ms` seems to be the right server option, but it was deprecated in favor of
`keep-alive`: https://aerospike.com/docs/database/reference/config/#service__proto-fd-idle-ms
