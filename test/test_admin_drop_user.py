# -*- coding: utf-8 -*-

import pytest
import sys
import time
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class SomeClass(object):

    pass


class TestDropUser(object):

    def setup_method(self, method):

        """
        Setup method.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        self.client = aerospike.client(config).connect( "admin", "admin" )

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        self.client.close()

    def test_drop_user_with_no_parameters(self):
        """
            Invoke drop_user() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_user()

        assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_drop_user_with_only_policy(self):

        """
            Invoke drop_user() with only policy.
        """
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_user( policy )

        assert "Required argument 'user' (pos 2) not found" in typeError.value

    def test_drop_user_with_policy_none(self):

        """
            Invoke drop_user() with policy none
        """
        policy = None
        user = "foo"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        time.sleep(2)

        assert status == 0
        user_details = self.client.admin_query_user( policy, user )

        assert user_details == [{'roles': ['sys-admin', 'read', 'read-write',
], 'roles_size': 3, 'user': 'foo'}]

        status = self.client.admin_drop_user( policy, user )

        assert status == 0

        with pytest.raises(Exception) as exception:
            user_details = self.client.admin_query_user( policy, user )

        assert exception.value[0] == 60L
        assert exception.value[1] == 'aerospike query user failed'

    def test_drop_user_with_user_none(self):

        """
            Invoke drop_user() with policy none
        """
        policy = {
            'timeout': 1000
        }
        with pytest.raises(Exception) as exception:
            self.client.admin_drop_user( policy, None )

        assert exception.value[0] == -2L
        assert exception.value[1] == 'Username should be a string'

    def test_drop_user_positive(self):

        """
            Invoke drop_user() with correct arguments.
        """
        policy = {
            'timeout': 1000
        }
        user = "foo"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        time.sleep(2)

        assert status == 0
        user_details = self.client.admin_query_user( policy, user )

        assert user_details == [{'roles': ['sys-admin', 'read', 'read-write',
], 'roles_size': 3, 'user': 'foo'}]
        status = self.client.admin_drop_user( policy, user )
        assert status == 0

        with pytest.raises(Exception) as exception:
            user_details = self.client.admin_query_user( policy, user )

        assert exception.value[0] == 60L
        assert exception.value[1] == 'aerospike query user failed'

    def test_drop_user_negative(self):

        """
            Invoke drop_user() with non-existent user.
        """
        policy = {}
        user = "foo"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]
        with pytest.raises(Exception) as exception:
            user_details = self.client.admin_query_user( policy, user )

        assert exception.value[0] == 60L
        assert exception.value[1] == 'aerospike query user failed'

        with pytest.raises(Exception) as exception:
            status = self.client.admin_drop_user( policy, user )

        assert exception.value[0] == 60L
        assert exception.value[1] == 'aerospike drop user failed'

    def test_drop_user_policy_incorrect(self):

        """
            Invoke drop_user() with policy incorrect
        """
        policy = {
            'timeout': 1000
        }
        user = "incorrect-policy"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        time.sleep(2)

        assert status == 0
        user_details = self.client.admin_query_user( policy, user )

        assert user_details == [{'roles': ['sys-admin', 'read', 'read-write',
], 'roles_size': 3, 'user': 'incorrect-policy'}]
        policy = {
            'timeout': 0.2
        }
        with pytest.raises(Exception) as exception:
            status = self.client.admin_drop_user( policy, user )

        assert exception.value[0] == -2L
        assert exception.value[1] == 'timeout is invalid'

        status = self.client.admin_drop_user( {}, user )

    def test_drop_user_with_extra_argument(self):

        """
            Invoke drop_user() with extra argument.
        """
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_user( policy, "foo", "" )

        assert "admin_drop_user() takes at most 2 arguments (3 given)" in typeError.value

    def test_drop_user_with_too_long_username(self):

        policy = {}
        user = "user$"*1000
        password = "user10"
        roles = [ "sys-admin" ]

        with pytest.raises(Exception) as exception:
            status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        assert exception.value[0] == 60
        assert exception.value[1] == "aerospike create user failed"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_drop_user( policy, user )

        assert exception.value[0] == 60
        assert exception.value[1] == "aerospike drop user failed"

    def test_drop_user_with_special_characters_in_username(self):

        policy = {}
        user = "!#Q#AEQ@#$%&^*((^&*~~~````"
        password = "user4"
        roles = ["read-write"]

        status = self.client.admin_create_user( policy, user, password, roles, len(roles) )

        assert status == 0

        status = self.client.admin_drop_user( policy, user )

        assert status == 0

