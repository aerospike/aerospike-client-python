# This checks that the client config values are being set without any memory leaks or errors
# It doesn't test the client config values themselves work properly with the server
# It still needs a lot of work, such as assigning more config values
# It's only testing specific Jira tickets that come up

import aerospike
from .test_base_class import TestBaseClass


class TestClose:
    def test_client_config_rack_aware(self):
        config = TestBaseClass.get_connection_config()
        config["rack_aware"] = True
        config["rack_id"] = 1
        config["policies"]["batch"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
        config["policies"]["scan"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
        config["policies"]["query"]["replica"] = aerospike.POLICY_REPLICA_PREFER_RACK
        aerospike.client(config)
