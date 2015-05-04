# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")


class TestQueryUser(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass().get_hosts()[1] == None,
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {"hosts": hostlist}
        TestQueryUser.Me = self
        self.client = aerospike.client(config).connect(user, password)

        policy = {}
        user = "example"
        password = "foo2"
        roles = ["read-write", "sys-admin", "read"]

        status = self.client.admin_create_user( user, password, roles, policy )

        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        policy = {}

        self.client.admin_drop_user( "example", policy )

        self.client.close()

    def test_query_user_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_user()

        assert "Required argument 'user' (pos 1) not found" in typeError.value

    def test_query_user_with_proper_parameters(self):

        policy = {}
        user = "example"

        time.sleep(2)
        user_details = self.client.admin_query_user( user )

        assert user_details == [{'roles': ['read', 'read-write', 'sys-admin'], 'roles_size':
3, 'user': user}]

    def test_query_user_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "example"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_query_user( user, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_query_user_with_proper_timeout_policy_value(self):

        policy = {'timeout': 5}
        user = "example"

        time.sleep(2)
        user_details = self.client.admin_query_user( user, policy )

        assert user_details == [{'roles': ['read','read-write','sys-admin'],
'roles_size': 3, 'user': user}]

    def test_query_user_with_none_username(self):

        policy = {'timeout': 0}
        user = None

        with pytest.raises(Exception) as exception:
            user_details = self.client.admin_query_user( user, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "Username should be a string"

    def test_query_user_with_empty_username(self):

        policy = {}
        user = ""

        with pytest.raises(Exception) as exception:
            status = self.client.admin_query_user( user, policy )

        assert exception.value[0] == 60
        assert exception.value[1] == "AEROSPIKE_INVALID_USER"

    def test_query_user_with_nonexistent_username(self):

        policy = {}
        user = "non-existent"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_query_user( user, policy )

        assert exception.value[0] == 60
        assert exception.value[1] == "AEROSPIKE_INVALID_USER"

    def test_query_user_with_no_roles(self):

        policy = {}
        user = "example"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(user, roles, policy)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user( user )

        assert user_details == [
            {'roles': [],
             'roles_size': 0,
             'user': 'example'}
        ]

    def test_query_user_with_extra_argument(self):
        """
            Invoke query_user() with extra argument.
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_user( "foo", policy, "" )

        assert "admin_query_user() takes at most 2 arguments (3 given)" in typeError.value

    def test_query_user_with_policy_as_string(self):
        """
            Invoke query_user() with policy as string
        """
        policy = ""
        with pytest.raises(Exception) as exception:
            self.client.admin_query_user( "foo", policy)

        assert exception.value[0] == -2L
        assert exception.value[1] == "policy must be a dict"
