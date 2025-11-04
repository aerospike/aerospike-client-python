# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e
from contextlib import nullcontext
import aerospike
from .conftest import poll_until_user_doesnt_exist, admin_create_user_and_poll


@pytest.mark.usefixtures("connection_config")
class TestCreateUser(object):
    user = "user7"

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        if not TestBaseClass.auth_in_use():
            pytest.skip("No user specified, may be not secured cluster.")

        self.client = aerospike.client(config).connect(
            config["user"], config["password"]
        )

        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        for user in self.delete_users:
            try:
                self.client.admin_drop_user(user)
                poll_until_user_doesnt_exist(username=user, client=self.client)
            except Exception:
                pass
        self.client.close()

    def test_create_user_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_create_user()

        assert "argument 'user' (pos 1)" in str(typeError.value)

    def test_create_user_with_proper_parameters(self):

        policy = {"timeout": 180000}
        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            poll_until_user_doesnt_exist(user, self.client)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles, policy)

        time.sleep(2)

        assert status == 0

        user = self.client.admin_query_user_info(user, policy)

        assert user["roles"] == ["read", "read-write", "sys-admin"]

        self.delete_users.append("user1-test")

    def test_create_user_with_proper_parameters_without_policy(self):

        user = "user1-test"
        password = "user1-test"
        roles = ["read", "read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles)

        time.sleep(2)

        assert status == 0

        user = self.client.admin_query_user_info(user)

        assert user["roles"] == ["read", "read-write", "sys-admin"]

        self.delete_users.append("user1-test")

    def test_create_user_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "user3-test"
        password = "user3-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except Exception:
            pass

        try:
            admin_create_user_and_poll(self.client, user, password, roles, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_create_user_with_proper_timeout_policy_value(self):

        policy = {"timeout": 180000}
        user = "user2-test"
        password = "user2-test"
        roles = ["read-write", "sys-admin"]

        try:
            self.client.admin_drop_user(user, policy)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles, policy)

        time.sleep(2)

        assert status == 0

        user = self.client.admin_query_user_info(user)

        assert user["roles"] == ["read-write", "sys-admin"]

        self.delete_users.append("user2-test")

    def test_create_user_with_none_username(self):

        user = None
        password = "user3-test"
        roles = ["sys-admin"]

        try:
            admin_create_user_and_poll(self.client, user, password, roles)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_create_user_with_empty_username(self):

        user = ""
        password = "user3-test"
        roles = ["read-write"]

        try:
            admin_create_user_and_poll(self.client, user, password, roles)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_create_user_with_special_characters_in_username(self):

        user = "!#Q#AEQ@#$%&^*((^&*~~~````"
        password = "uesr4-test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles)

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_none_password(self):

        user = "uesr4-test"
        password = None
        roles = ["sys-admin"]

        try:
            admin_create_user_and_poll(self.client, user, password, roles)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_create_user_with_empty_string_as_password(self):

        user = "user5-test"
        password = ""
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles)

        assert status == 0
        time.sleep(2)
        self.delete_users.append(user)

    def test_create_user_with_special_characters_in_password(self):

        user = "user6-test"
        password = "@#!$#$WERWE%&%$"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles)

        assert status == 0

        self.delete_users.append(user)

    def test_create_user_with_too_long_username(self):

        user = "user$" * 1000
        password = "user10-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        try:
            admin_create_user_and_poll(self.client, user, password, roles)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

        except e.ClientError:
            pass

    def test_create_user_with_too_long_password(self):

        user = "user10-test"
        password = "user#" * 1000
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        with pytest.raises(e.ClientError):
            admin_create_user_and_poll(self.client, user, password, roles)

    def test_create_user_with_empty_roles_list(self):

        user = "user7"
        password = "user7"
        roles = []

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        try:
            admin_create_user_and_poll(self.client, user, password, roles)

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_create_user_with_non_user_admin_user(self):

        user = "non_admin_test"
        password = "non_admin_test"
        roles = ["read-write"]

        try:
            self.client.admin_drop_user(user)
            time.sleep(2)
        except Exception:
            pass

        status = admin_create_user_and_poll(self.client, user, password, roles)
        time.sleep(2)

        assert status == 0

        config = self.connection_config

        non_admin_client = None

        try:
            # Close and reconnect with non_admin_test user
            non_admin_client = aerospike.client(config)
            non_admin_client.close()
            non_admin_client.connect("non_admin_test", "non_admin_test")
            status = non_admin_client.admin_create_user("user78", password, roles)

            if non_admin_client:
                non_admin_client.close()

        except e.RoleViolation as exception:
            assert exception.code == 81

        self.delete_users.append("non_admin_test")

    @pytest.mark.parametrize("roles", [{}, (), 5, "read-write"])
    def test_create_user_with_non_list_roles(self, roles):

        user = "user7"
        password = "user7"
        try:
            self.client.admin_drop_user(user)
        except Exception:
            pass

        with pytest.raises(e.ParamError):
            admin_create_user_and_poll(self.client, user, password, roles)

    @pytest.mark.parametrize("list_item", [{}, (), 5, []])
    def test_create_user_with_invalid_roles_types(self, list_item):

        user = "user7"
        password = "user7"
        roles = ["read-write", list_item]
        try:
            self.client.admin_drop_user(user)
        except Exception:
            pass

        with pytest.raises(e.ClientError):
            admin_create_user_and_poll(self.client, user, password, roles)

    def test_create_user_with_very_long_role_name(self):

        password = "user7"
        roles = ["read-write", "abc" * 50]
        try:
            self.client.admin_drop_user(self.user)
            time.sleep(2)
        except Exception:
            pass

        with pytest.raises(e.ClientError):
            admin_create_user_and_poll(self.client, self.user, password, roles)

    # Need as_connection to get server version
    def test_create_pki_user(self, as_connection):
        try:
            self.client.admin_drop_user(self.user)
            time.sleep(2)
        except Exception:
            pass

        self.delete_users.append(self.user)

        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) < (8, 1):
            context = pytest.raises(e.AerospikeError)
        else:
            context = nullcontext()

        # Make sure mutual TLS is enabled.
        if not (
            TestBaseClass.tls_in_use()
            and "tls" in self.connection_config
            and "certfile" in self.connection_config["tls"]
        ):
            pytest.skip("Mutual TLS is not enabled")

        roles = ["read-write"]
        admin_policy = {}
        with context:
            self.client.admin_create_pki_user(
                user=self.user, roles=roles, policy=admin_policy
            )

        if type(context) == nullcontext:
            print("Check that the PKI user was created.")
            time.sleep(2)
            userDict = self.client.admin_query_user_info(self.user)
            assert userDict["roles"] == ["read-write"]
