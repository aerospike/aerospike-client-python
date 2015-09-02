# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestCreateUser(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass().get_hosts()[1] == None,
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {"hosts": hostlist}

        self.client = aerospike.client(config).connect(user, password)

        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        policy = {}

        for user in self.delete_users:
            self.client.admin_drop_user( user, policy )

        self.client.close()

    def test_create_user_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_create_user()

        assert "Required argument 'user' (pos 1) not found" in typeError.value

    def test_create_user_with_proper_parameters(self):

        policy = {"timeout": 1000}
        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user( user, policy )

        assert user_details == ['read', 'read-write', 'sys-admin']

        self.delete_users.append('user1-test')

    def test_create_user_with_proper_parameters_without_policy(self):

        policy = { "timeout": 1000 }
        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles )

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user( user, policy )

        assert user_details == ['read', 'read-write', 'sys-admin']

        self.delete_users.append('user1-test')

    def test_create_user_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "user3-test"
        password = "user3-test"
        roles = ['sys-admin']

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_create_user_with_proper_timeout_policy_value(self):

        policy = {'timeout': 20}
        user = "user2-test"
        password = "user2-test"
        roles = ["read-write", "sys-admin"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles , policy )

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user( user )

        assert user_details == ['read-write', 'sys-admin']

        self.delete_users.append('user2-test')

    def test_create_user_with_none_username(self):

        policy = {'timeout': 20}
        user = None
        password = "user3-test"
        roles = ["sys-admin"]

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_create_user_with_empty_username(self):

        policy = {}
        user = ""
        password = "user3-test"
        roles = ["read-write"]

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_create_user_with_special_characters_in_username(self):

        policy = {}
        user = "!#Q#AEQ@#$%&^*((^&*~~~````"
        password = "uesr4-test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_none_password(self):

        policy = {}
        user = "uesr4-test"
        password = None
        roles = ["sys-admin"]

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_create_user_with_empty_string_as_password(self):

        policy = {}
        user = "user5-test"
        password = ""
        roles = ["read-write"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        assert status == 0
        time.sleep(2)
        self.delete_users.append(user)

    def test_create_user_with_special_characters_in_password(self):

        policy = {}
        user = "user6-test"
        password = "@#!$#$WERWE%&%$"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_too_long_username(self):

        policy = {}
        user = "user$" * 1000
        password = "user10-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_create_user_with_too_long_password(self):

        policy = {'timeout': 1000}
        user = "user10-test"
        password = "user#" * 1000
        roles = ["read-write"]

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        assert status == 0
        time.sleep(1)

        user_details = self.client.admin_query_user( user, policy )

        assert user_details == ['read-write']

        self.delete_users.append(user)

    def test_create_user_with_empty_roles_list(self):

        policy = {}
        user = "user7"
        password = "user7"
        roles = []

        try:
            self.client.admin_drop_user ( user, policy )
        except:
            pass

        try:
            status = self.client.admin_create_user( user, password, roles, policy )

        except InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_create_user_with_non_user_admin_user(self):

        policy = {}
        user = "non_admin_test"
        password = "non_admin_test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user( user, policy )
        except:
            pass

        status = self.client.admin_create_user( user, password, roles, policy )

        assert status == 0

        config = {"hosts": TestCreateUser.hostlist}

        non_admin_client = None

        try:
            non_admin_client = aerospike.client(config).connect( "non_admin_test", "non_admin_test" )
            status = non_admin_client.admin_create_user( "user78", password, roles, policy )

            if non_admin_client:
                non_admin_client.close()

        except RoleViolation as exception:
            assert exception.code == 81

        self.delete_users.append("non_admin_test")
