# -*- coding: utf-8 -*-

import pytest
import sys
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestGrantRoles(object):

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
        roles = ["read-write"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """

        policy = {}

        self.client.admin_drop_user( policy, "example" )

        self.client.close()

    def test_grant_roles_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_grant_roles()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_grant_roles_with_proper_parameters(self):

        policy = {'timeout': 1000}
        user = "example"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_grant_roles(policy, user, roles, len(roles))
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user( policy, user )

        assert user_details == [{'roles': ['sys-admin', 'read', 'read-write',
], 'roles_size': 3, 'user': 'example'}]

    def test_grant_roles_with_invalid_timeout_policy_value(self):

        policy = { "timeout" : 0.1 }
        user = "example"
        roles = ['sys-admin']

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles( policy, user, roles, len(roles) )

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_grant_roles_with_proper_timeout_policy_value(self):

        policy = { 'timeout' : 5 }
        user = "example"
        roles = ["read-write", "sys-admin"]

        status = self.client.admin_grant_roles( policy, user, roles , len(roles) )

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user( {}, user )

        assert user_details == [{'roles': ['sys-admin', 'read-write',
], 'roles_size': 2, 'user': 'example'}]



    def test_grant_roles_with_none_username(self):

        policy = { 'timeout' : 0 }
        user = None
        roles = ["sys-admin"]

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles( policy, user, roles, len(roles) )

        assert exception.value[0] == -2
        assert exception.value[1] == "Username should be a string"

    def test_grant_roles_with_empty_username(self):

        policy = {'timeout': 1000}
        user = ""
        roles = ["read-write"]

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles( policy, user, roles, len(roles) )

        assert exception.value[0] == 60
        assert exception.value[1] == "aerospike grant roles failed"

    def test_grant_roles_with_special_characters_in_username(self):

        policy = {'timeout': 1000}
        user = "!#Q#AEQ@#$%&^*((^&*~~~````["
        password = "abcd"
        roles = ["read-write"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        assert status == 0
        roles = ["read"]
        status = self.client.admin_grant_roles( policy, user, roles , len(roles) )

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user( {}, user )

        assert user_details == [{'roles': ['read','read-write'], 'roles_size':
2, 'user':'!#Q#AEQ@#$%&^*((^&*~~~````['}]

        status = self.client.admin_drop_user( policy,
"!#Q#AEQ@#$%&^*((^&*~~~````[" )
        assert status == 0

    def test_grant_roles_with_empty_roles_list(self):

        policy = {'timeout': 1000}
        user = "example"
        roles = []

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles( policy, user, roles, len(roles) )

        assert exception.value[0] == 70
        assert exception.value[1] == "aerospike grant roles failed"

    def test_grant_roles_with_invalid_role(self):

        policy = {'timeout': 1000}
        user = "example"
        roles = ["viewer"]

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles( policy, user, roles, len(roles) )

        assert exception.value[0] == 70
        assert exception.value[1] == "aerospike grant roles failed"

    def test_grant_roles_with_different_roles_and_roles_size(self):

        policy = {'timeout': 1000}
        user = "example"
        roles = ["read-write"]

        with pytest.raises(Exception) as exception:
            status = self.client.admin_grant_roles(policy, user, roles, 2)

        assert exception.value[0] == 70
        assert exception.value[1] == "aerospike grant roles failed"
