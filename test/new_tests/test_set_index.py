import pytest
import aerospike
from aerospike import exception as e
from .index_helpers import ensure_dropped_index
from .test_base_class import TestBaseClass
import time


INDEX_NAME = "index_name"

@pytest.mark.usefixtures("as_connection")
class TestSetIndex:
    @pytest.fixture
    def client_as_sindex_admin_user(self):
        if (not TestBaseClass.auth_in_use()):
            pytest.skip("Security required to create a user with the sindex-admin role")

        USERNAME_AND_PASSWORD = "user_with_sindex_admin"
        try:
            self.as_connection.admin_drop_user(
                user=USERNAME_AND_PASSWORD,
            )
            time.sleep(2)
        except e.InvalidUser:
            pass

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
        # TODO: clean up steps should also be run here?

    def test_create_set_index(self, client_as_sindex_admin_user):
        client_as_sindex_admin_user.index_set_create("test", "demo", INDEX_NAME)
