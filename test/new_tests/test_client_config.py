# This checks that the client config values are being set without any memory leaks or errors
# It doesn't test the client config values themselves work properly with the server
# It still needs a lot of work, such as assigning more config values
# It's only testing specific Jira tickets that come up

import aerospike
from .test_base_class import TestBaseClass


class TestClose:
    def test_client_batch_policy_replica(self):
        config = TestBaseClass.get_connection_config()
        config["policies"]["batch"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
        aerospike.client(config)
