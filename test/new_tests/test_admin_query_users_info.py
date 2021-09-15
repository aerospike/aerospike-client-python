# -*- coding: utf-8 -*-

import pytest
import sys
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestQueryUsersInfo(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may not be secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestQueryUsersInfo.Me = self
        self.client = aerospike.client(config).connect(config['user'], config['password'])

        try:
            self.client.admin_drop_user("example-test")
            time.sleep(2)
        except e.InvalidUser:
            pass
        policy = {}
        user = "example-test"
        password = "foo2"
        roles = ["read-write", "sys-admin", "read"]

        try:
            self.client.admin_create_user(user, password, roles, policy)
            time.sleep(2)
        except e.UserExistsError:
            pass
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        policy = {}

        try:
            self.client.admin_drop_user("example-test", policy)
        except:
            pass
        self.client.close()

    def test_query_users_info_with_proper_parameters(self):

        time.sleep(2)
        user_details = self.client.admin_query_users_info()

        assert user_details.get(
            'example-test').get('roles') == ['read', 'read-write', 'sys-admin']

    def test_query_users_info_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}

        try:
            self.client.admin_query_users_info(policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_query_users_info_with_proper_timeout_policy_value(self):

        policy = {'timeout': 50}

        user_details = self.client.admin_query_users_info(policy)

        assert user_details.get(
            'example-test').get('roles') == ['read', 'read-write', 'sys-admin']

    def test_query_users_info_with_no_roles(self):

        policy = {}
        user = "example-test"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(user, roles, policy)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_users_info(policy)

        assert user_details.get('example-test').get('roles') == []

    def test_query_users_info_with_extra_argument(self):
        """
            Invoke query_users() with extra argument.
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_users_info(policy, "")

        assert "admin_query_users_info() takes at most 1 argument (2 given)" in str(
            typeError.value)

    def test_query_users_info_with_policy_as_string(self):
        """
            Invoke query_users() with policy as string
        """
        policy = ""
        try:
            self.client.admin_query_users_info(policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"
