# -*- coding: utf-8 -*-

import pytest
import sys
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestQueryUsers(object):

    def setup_method(self, method):

        """
        Setup method
        """
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        self.client = aerospike.client(config).connect( "admin", "admin" )

        policy = {}
        user = "example"
        password = "foo2"
        roles = ["read-write", "sys-admin", "read"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """

        policy = {}

        self.client.admin_drop_user( policy, "example" )

        self.client.close()

    def test_query_users_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_users()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_query_users_with_proper_parameters(self):

        policy = {}

        time.sleep(2)
        user_details = self.client.admin_query_users( policy)

        for user in user_details:
            if user['user'] == "example":
                assert user == {'roles': ['sys-admin', 'read', 'read-write'], 'roles_size':
3, 'user': "example"}

    def test_query_users_with_invalid_timeout_policy_value(self):

        policy = { "timeout" : 0.1 }
        user = "example"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_query_users( policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_query_users_with_proper_timeout_policy_value(self):

        policy = { 'timeout' : 5 }

        time.sleep(2)
        user_details = self.client.admin_query_users( policy )

        time.sleep(2)
        for user in user_details:
            if user['user'] == "example":
                assert user == {'roles': ['sys-admin','read','read-write'],
'roles_size': 3, 'user': "example"}

    def test_query_users_with_no_roles(self):

        policy = {}
        user = "example"
        roles = ["sys-admin", "read", "read-write"]

        status = self.client.admin_revoke_roles(policy, user, roles, len(roles))
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_users( policy )

        time.sleep(2)
        for user in user_details:
            if user['user'] == "example":
                assert user == {'roles': [],
'roles_size': 0, 'user': "example"}

    def test_query_users_with_extra_argument(self):

        """
            Invoke query_users() with extra argument.
        """
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.admin_query_users( policy, "" )

        assert "admin_query_users() takes at most 1 argument (2 given)" in typeError.value

    def test_query_users_with_policy_as_string(self):

        """
            Invoke query_users() with policy as string
        """
        policy = ""
        with pytest.raises(Exception) as exception:
            self.client.admin_query_users( policy )

        assert exception.value[0] == -2L
        assert exception.value[1] == "policy must be a dict"
