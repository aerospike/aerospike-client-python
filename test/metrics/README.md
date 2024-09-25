These tests are designed to run manually. The automated tests in `new_tests` are designed to pass on any server
configuration, but there are certain features in metrics that rely on certain server behavior or configurations out of
the control of those automated tests.

## How to run tests

- `test_node_close_listener.py` requires both Docker and Aerolab configured to use the former. If tweaking the test to
use multiple nodes, aerolab must also be configured to use a valid `features.conf`.

Reason for test: `new_tests` does not manipulate the server.

- `test_cluster_name.py` only requires Docker.

Reason for test: The client in `new_tests` does not expect a cluster name. Also, the cluster name can change depending
on the server configuration.
