import unittest
import time
import aerospike
from aerospike import exception as e
from contextlib import nullcontext

# Creating a PKI user with the superuser doesn't work
# I believe it's because the user creating a PKI user must only have the user-admin role
# The superuser has many other roles for testing and setting up strong consistency in the entrypoint script.
config = {
    "hosts": [
        ("127.0.0.1", 4333, "docker")
    ],
    # "user": "pki1",
    "policies": {
        "auth_mode": aerospike.AUTH_PKI
    },
    "tls": {
        "enable": True,
        "cafile": "../../.github/workflows/docker-build-context/ca.cer",
        "keyfile": "../../.github/workflows/docker-build-context/client.pem",
        "certfile": "../../.github/workflows/docker-build-context/client.cer"
    }
}


class TestCreatePKIUser(unittest.TestCase):
    def test_create_pki_user(self):
        print("Connecting to server...")
        as_client = aerospike.client(config)

        try:
            pki_user = "pki2"
            roles = ["read-write"]
            admin_policy = {}
            info_response = as_client.info_random_node("build")
            print(info_response)
            # Example response format: build <version>
            tokens = info_response.split()
            server_version = tokens[1]
            if server_version.startswith('8.1'):
                context = nullcontext()
            else:
                context = self.assertRaises(e.AerospikeError)

            with context:
                as_client.admin_create_pki_user(user=pki_user, roles=roles, policy=admin_policy)

            if type(context) == nullcontext:
                time.sleep(3)
                # Check that the PKI user was created.
                userDict = as_client.admin_query_user_info(pki_user)
                assert userDict["roles"] == ["read-write"]
                print("PKI user created successfully.")
            else:
                print("admin_create_pki_user() failed as expected for unsupported server versions.")
        finally:
            # Cleanup
            as_client.close()


if __name__ == '__main__':
    unittest.main()
