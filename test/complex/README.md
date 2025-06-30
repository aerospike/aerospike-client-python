## Rationale for these tests

The automated tests in `new_tests` are designed to pass on any server
configuration, but there are certain features that rely on certain server behavior or configurations out of
the control of those automated tests. Also, the user that is used for the tests may not have the right permissions,
so we need to log in as a different user.

## How to run tests

- `test_node_close_listener.py` requires both Docker and Aerolab configured to use the former. If tweaking the test to
use multiple nodes, aerolab must also be configured to use a valid `features.conf`.

Reason for test: `new_tests` does not manipulate the server.

- `test_cluster_name.py` only requires Docker.

Reason for test: The client in `new_tests` does not expect a cluster name. Also, the cluster name can change depending
on the server configuration.

- `test_create_pki_user.py` requires the enterprise edition server test image from `.github/workflows/docker-build-context`
to be run. Creating a PKI user with the superuser doesn't work (probably) because the user creating a PKI user must only
have the `user-admin` role. The superuser has many other roles for testing and setting up strong consistency in the
entrypoint script, so we cannot afford to remove the other roles.
