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

        ensure_dropped_index(self.as_connection, "test", INDEX_NAME)

    @pytest.mark.parametrize(
        "expect_earlier_than_server_version_to_fail",
        [
            (8, 1, 2)
        ],
        indirect=True
    )
    def test_create_set_index(self, client_as_sindex_admin_user, expect_earlier_than_server_version_to_fail):
        with self.expected_context_for_pos_tests:
            client_as_sindex_admin_user.index_set_create(ns="test", set="demo", name=INDEX_NAME, policy=None)

    def test_create_set_index_with_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.index_set_create("test", "demo")
