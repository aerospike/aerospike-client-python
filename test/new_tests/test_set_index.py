import pytest
import aerospike
from .index_helpers import ensure_dropped_index
from .test_base_class import TestBaseClass


INDEX_NAME = "index_name"

@pytest.mark.usefixtures("as_connection")
class TestSetIndex:
    @pytest.fixture
    def client_as_sindex_admin_user(self):
        USERNAME_AND_PASSWORD = "user_with_sindex_admin"
        self.as_connection.admin_create_user(
            user=USERNAME_AND_PASSWORD,
            password=USERNAME_AND_PASSWORD,
            roles=[
                "sindex-admin"
            ]
        )

        config = TestBaseClass.get_connection_config()
        config["user"] = USERNAME_AND_PASSWORD
        config["password"] = USERNAME_AND_PASSWORD

        yield aerospike.client(config)

        ensure_dropped_index(self.as_connection, None, INDEX_NAME)
        self.as_connection.admin_drop_user(
            user=USERNAME_AND_PASSWORD,
        )

    def test_create_set_index(self, client_as_sindex_admin_user):
        client_as_sindex_admin_user.index_set_create("test", "demo", INDEX_NAME)
