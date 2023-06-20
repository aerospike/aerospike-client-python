# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


@pytest.mark.skip("client.admin_query_users() is deprecated")
class TestQueryUsers(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestQueryUsers.Me = self
        self.client = aerospike.client(config).connect(config["user"], config["password"])

        try:
            self.client.admin_drop_user("example-test")
            time.sleep(2)
        except e.InvalidUser:
            pass
        user = "example-test"
        password = "foo2"
        roles = ["read-write", "sys-admin", "read"]

        try:
            self.client.admin_create_user(user, password, roles)
            time.sleep(2)
        except e.UserExistsError:
            pass
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        try:
            self.client.admin_drop_user("example-test")
        except Exception:
            pass
        self.client.close()

    def test_query_users_with_proper_parameters(self):

        time.sleep(2)
        user_details = self.client.admin_query_users()

        assert user_details["example-test"] == ["read", "read-write", "sys-admin"]

    def test_query_users_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}

        try:
            self.client.admin_query_users(policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_query_users_with_proper_timeout_policy_value(self):

        policy = {"timeout": 180000}

        time.sleep(2)
        user_details = self.client.admin_query_users(policy)

        time.sleep(2)
        assert user_details["example-test"] == ["read", "read-write", "sys-admin"]

    def test_query_users_with_no_roles(self):

        user = "example-test"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(user, roles)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_users()

        time.sleep(2)
        assert user_details["example-test"] == []

    def test_query_users_with_extra_argument(self):
        """
        Invoke query_users() with extra argument.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_users(None, "")

        assert "admin_query_users() takes at most 1 argument (2 given)" in str(typeError.value)

    def test_query_users_with_policy_as_string(self):
        """
        Invoke query_users() with policy as string
        """
        policy = ""
        try:
            self.client.admin_query_users(policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"
