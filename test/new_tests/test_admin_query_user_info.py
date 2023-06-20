# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


class TestQueryUserInfo(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may not be secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestQueryUserInfo.Me = self
        self.client = aerospike.client(config).connect(config["user"], config["password"])
        try:
            self.client.admin_drop_user("example-test")
            time.sleep(1)
        except e.InvalidUser:
            pass
        user = "example-test"
        password = "foo2"
        roles = ["read-write", "sys-admin", "read"]

        try:
            self.client.admin_create_user(user, password, roles)
            time.sleep(1)
        except e.UserExistsError:
            pass
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        try:
            self.client.admin_drop_user("example-test")
            time.sleep(1)
        except e.InvalidUser:
            pass

        self.client.close()

    def test_query_user_info_without_any_parameters(self):

        with pytest.raises(TypeError):
            self.client.admin_query_user_info()

    def test_query_user_info_with_proper_parameters(self):

        user = "example-test"

        time.sleep(2)
        user_details = self.client.admin_query_user_info(user)
        assert user_details.get("roles") == [
            "read",
            "read-write",
            "sys-admin"
        ]
        # The user has not read or written anything, so all r/w stats should be 0
        # NOTE: we don't test the scenario where read_info / write_info is not 0
        # because it takes time and a lot of transactions for the server to actually record non-zero values
        assert user_details.get("read_info") == 0
        assert user_details.get("write_info") == 0
        # No clients were logged in as this user
        assert user_details.get("conns_in_use") == 0

    def test_query_user_info_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "example-test"

        try:
            self.client.admin_query_user_info(user, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_query_user_info_with_proper_timeout_policy_value(self):

        policy = {"timeout": 180000}
        user = "example-test"

        time.sleep(2)
        user_details = self.client.admin_query_user_info(user, policy)

        assert user_details.get("roles") == ["read", "read-write", "sys-admin"]

    def test_query_user_info_with_none_username(self):

        user = None

        try:
            self.client.admin_query_user_info(user)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_query_user_info_with_empty_username(self):

        user = ""

        try:
            self.client.admin_query_user_info(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_query_user_info_with_nonexistent_username(self):

        user = "non-existent"

        try:
            self.client.admin_query_user_info(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_query_user_info_with_no_roles(self):

        user = "example-test"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(user, roles)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user_info(user)

        assert user_details.get("roles") == []

    def test_query_user_info_with_extra_argument(self):
        """
        Invoke query_user() with extra argument.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_user_info("foo", None, "")

        assert "admin_query_user_info() takes at most 2 arguments (3 given)" in str(typeError.value)

    def test_query_user_info_with_policy_as_string(self):
        """
        Invoke query_user() with policy as string
        """
        policy = ""
        try:
            self.client.admin_query_user_info("foo", policy)

        except e.AerospikeError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"
