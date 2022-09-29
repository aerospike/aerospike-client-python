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

@pytest.mark.usefixtures("connection_config")
class TestCreateUser(object):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()

        self.client = aerospike.client(config).connect(config['user'], config['password'])

        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        policy = {}

        for user in self.delete_users:
            try:
                self.client.admin_drop_user(user, policy)
            except:
                pass
        time.sleep(2)
        self.client.close()

    def test_create_user_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_create_user()

        assert "argument 'user' (pos 1)" in str(
            typeError.value)

    def test_create_user_with_proper_parameters(self):

        policy = {"timeout": 1000}
        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']

        self.delete_users.append('user1-test')

    def test_create_user_with_proper_parameters_without_policy(self):

        policy = {"timeout": 1000}
        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles)

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']

        self.delete_users.append('user1-test')

    def test_create_user_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "user3-test"
        password = "user3-test"
        roles = ['sys-admin']

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_create_user_with_proper_timeout_policy_value(self):

        policy = {'timeout': 20}
        user = "user2-test"
        password = "user2-test"
        roles = ["read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user(user)

        assert user_details == ['read-write', 'sys-admin']

        self.delete_users.append('user2-test')

    def test_create_user_with_none_username(self):

        policy = {'timeout': 20}
        user = None
        password = "user3-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_create_user_with_empty_username(self):

        policy = {}
        user = ""
        password = "user3-test"
        roles = ["read-write"]

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_create_user_with_special_characters_in_username(self):

        policy = {}
        user = "!#Q#AEQ@#$%&^*((^&*~~~````"
        password = "uesr4-test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_none_password(self):

        policy = {}
        user = "uesr4-test"
        password = None
        roles = ["sys-admin"]

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_create_user_with_empty_string_as_password(self):

        policy = {}
        user = "user5-test"
        password = ""
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)

        assert status == 0
        time.sleep(2)
        self.delete_users.append(user)

    def test_create_user_with_special_characters_in_password(self):

        policy = {}
        user = "user6-test"
        password = "@#!$#$WERWE%&%$"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_too_long_username(self):

        policy = {}
        user = "user$" * 1000
        password = "user10-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

        except e.ClientError:
            pass

    def test_create_user_with_too_long_password(self):

        policy = {'timeout': 1000}
        user = "user10-test"
        password = "user#" * 1000
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        with pytest.raises(e.ClientError):
            self.client.admin_create_user(user, password, roles, policy)

    def test_create_user_with_empty_roles_list(self):

        policy = {}
        user = "user7"
        password = "user7"
        roles = []

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_create_user_with_non_user_admin_user(self):

        policy = {}
        user = "non_admin_test"
        password = "non_admin_test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        status = self.client.admin_create_user(user, password, roles, policy)
        time.sleep(2)

        assert status == 0

        config = self.connection_config

        non_admin_client = None

        try:
            non_admin_client = aerospike.client(config).connect(
                "non_admin_test", "non_admin_test")
            status = non_admin_client.admin_create_user(
                "user78", password, roles, policy)

            if non_admin_client:
                non_admin_client.close()

        except e.RoleViolation as exception:
            assert exception.code == 81

        self.delete_users.append("non_admin_test")

    @pytest.mark.parametrize(
        "roles",
        [
            {},
            (),
            5,
            "read-write"
        ])
    def test_create_user_with_non_list_roles(self, roles):

        policy = {}
        user = "user7"
        password = "user7"
        try:
            self.client.admin_drop_user(user, policy)
        except:
            pass

        with pytest.raises(e.ParamError):
            self.client.admin_create_user(user, password, roles)

    @pytest.mark.parametrize(
        "list_item",
        [
            {},
            (),
            5,
            []
        ])
    def test_create_user_with_invalid_roles_types(self, list_item):

        policy = {}
        user = "user7"
        password = "user7"
        roles = ['read-write', list_item]
        try:
            self.client.admin_drop_user(user, policy)
        except:
            pass

        with pytest.raises(e.ClientError):
            self.client.admin_create_user(user, password, roles)

    def test_create_user_with_very_long_role_name(self):

        policy = {}
        user = "user7"
        password = "user7"
        roles = ['read-write', "abc" * 50]
        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except:
            pass

        with pytest.raises(e.ClientError):
            self.client.admin_create_user(user, password, roles)
