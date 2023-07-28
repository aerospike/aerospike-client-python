# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


@pytest.mark.skip("client.admin_query_user() is deprecated")
class TestQueryUser(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestQueryUser.Me = self
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

    def test_query_user_without_any_parameters(self):

        with pytest.raises(TypeError):
            self.client.admin_query_user()

    def test_query_user_with_proper_parameters(self):

        user = "example-test"

        time.sleep(2)
        user_details = self.client.admin_query_user(user)
        assert user_details == ["read", "read-write", "sys-admin"]

    def test_query_user_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "example-test"

        try:
            self.client.admin_query_user(user, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_query_user_with_proper_timeout_policy_value(self):

        policy = {"timeout": 180000}
        user = "example-test"

        time.sleep(2)
        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ["read", "read-write", "sys-admin"]

    def test_query_user_with_none_username(self):

        user = None

        try:
            self.client.admin_query_user(user)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_query_user_with_empty_username(self):

        user = ""

        try:
            self.client.admin_query_user(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_query_user_with_nonexistent_username(self):

        user = "non-existent"

        try:
            self.client.admin_query_user(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_query_user_with_no_roles(self):

        user = "example-test"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(user, roles)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user(user)

        assert user_details == []

    def test_query_user_with_extra_argument(self):
        """
        Invoke query_user() with extra argument.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_user("foo", None, "")

        assert "admin_query_user() takes at most 2 arguments (3 given)" in str(typeError.value)

    def test_query_user_with_policy_as_string(self):
        """
        Invoke query_user() with policy as string
        """
        policy = ""
        try:
            self.client.admin_query_user("foo", policy)

        except e.AerospikeError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"
