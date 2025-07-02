## About

This Docker image deploys an Aerospike server with these features enabled by default:
- Strong consistency
- Security
- TLS mutual authentication

To disable any of the above features, start up the Docker container with any combination of these environment variables set:
```sh
# The value of the environment variable doesn't actually matter
NO_SC=1
NO_SECURITY=1
NO_TLS=1
```

## Note

The CA certificate in this folder is a fake certificate used both by the client and server for connecting via TLS.

The private key was generated along with this CA certificate, and is only used to sign the server certificate used in testing.

## How to build

Build the image:
```sh
docker build -f Dockerfile . --tag aerospike/aerospike-server-enterprise:latest-sc
```

Then run the image:
```sh
docker run -d -p 3000:3000 -p 4333:4333 --name aerospike aerospike/aerospike-server-enterprise:latest-sc
```

The container should take about 5 seconds to become "healthy":

```sh
% docker ps
CONTAINER ID   IMAGE                                             COMMAND                  CREATED        STATUS                           PORTS                                                           NAMES
376dbaf0f0a8   aerospike/aerospike-server-enterprise:latest-sc   "/usr/bin/as-tini-st…"   1 second ago   Up 1 second (health: starting)   0.0.0.0:3000->3000/tcp, 0.0.0.0:4333->4333/tcp, 3001-3002/tcp   aerospike
```

This shows the results of the entrypoint script which sets up strong consistency:
```sh
docker logs aerospike
```

The server logs are no longer printed to stdout; they are printed to this file in the container instead:
```sh
docker exec aerospike head -n 200 /var/log/aerospike/aerospike.log
```
