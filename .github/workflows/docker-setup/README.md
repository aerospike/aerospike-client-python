## About

The `run-ee-server.bash` script deploys an Aerospike server with the ability to enable these server features:
- Strong consistency
- Security
- TLS mutual authentication

To enable any of the above features, run this script with any combination of these environment variables set to:
```sh
STRONG_CONSISTENCY=1
SECURITY=1
MUTUAL_TLS=1
```

## Note

The CA certificate in this folder is a fake certificate used both by the client and server for connecting via TLS. The
private key was generated along with this CA certificate, and was only used to sign the server certificate used in testing.

The server certificate and private key is also provided here, so the setup script doesn't have to regenerate it every
time it gets run.

The client certificate and private key is also embedded here so the dev tests can reuse them when mutual authentication is enabled.

## Example of how to run

With only strong consistency enabled:
```sh
STRONG_CONSISTENCY=1 ./run-ee-server.bash
```

The script will return once the server is ready for testing.

## How to teardown

Run this script:
```sh
./teardown-ee-server.bash
```
