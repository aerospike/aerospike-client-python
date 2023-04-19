# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


class TestRevokeRoles(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestRevokeRoles.Me = self
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

    def test_revoke_roles_without_any_parameters(self):

        with pytest.raises(TypeError):
            self.client.admin_revoke_roles()

    def test_revoke_roles_with_proper_parameters(self):

        user = "example-test"
        roles = ["read", "sys-admin"]

        status = self.client.admin_revoke_roles(user, roles)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user(user)

        assert user_details == ["read-write"]

    def test_revoke_all_roles_with_proper_parameters(self):

        user = "example-test"
        roles = ["read", "sys-admin", "read-write"]

        time.sleep(2)
        status = self.client.admin_revoke_roles(user, roles)
        assert status == 0
        time.sleep(2)

        user_details = self.client.admin_query_user(user)

        assert user_details == []

    def test_revoke_roles_with_invalid_timeout_policy_value(self):

        policy = {"timeout": 0.1}
        user = "example-test"
        roles = ["sys-admin"]

        try:
            self.client.admin_revoke_roles(user, roles, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_revoke_roles_with_proper_timeout_policy_value(self):

        policy = {"timeout": 180000}
        user = "example-test"
        roles = ["read-write", "sys-admin"]

        status = self.client.admin_revoke_roles(user, roles, policy)

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user(user)

        assert user_details == ["read"]

    def test_revoke_roles_with_none_username(self):

        user = None
        roles = ["sys-admin"]

        try:
            self.client.admin_revoke_roles(user, roles)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_revoke_roles_with_empty_username(self):

        user = ""
        roles = ["read-write"]

        try:
            self.client.admin_revoke_roles(user, roles)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_revoke_roles_with_empty_roles_list(self):

        user = "example-test"
        roles = []

        try:
            self.client.admin_revoke_roles(user, roles)

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_revoke_roles_with_nonexistent_username(self):

        user = "non-existent"
        roles = ["read-write"]

        try:
            self.client.admin_revoke_roles(user, roles)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_revoke_roles_with_special_characters_in_username(self):

        user = "!#Q#AEQ@#$%&^*((^&*~~~````[["
        password = "abcd"
        roles = ["read-write"]

        status = self.client.admin_create_user(user, password, roles)
        time.sleep(2)

        assert status == 0
        status = self.client.admin_revoke_roles(user, roles)

        time.sleep(2)

        assert status == 0

        user_details = self.client.admin_query_user(user)

        assert user_details == []

        status = self.client.admin_drop_user("!#Q#AEQ@#$%&^*((^&*~~~````[[")
        assert status == 0

    def test_revoke_roles_nonpossessed(self):

        user = "examplenew"
        password = "abcd"
        roles = ["read-write"]

        status = self.client.admin_create_user(user, password, roles)
        time.sleep(2)

        assert status == 0
        roles = ["read"]
        status = self.client.admin_revoke_roles(user, roles)

        time.sleep(2)

        user_details = self.client.admin_query_user(user)

        assert user_details == ["read-write"]

        assert status == 0
        status = self.client.admin_drop_user(user)
        assert status == 0

    def test_revoke_roles_with_roles_exceeding_max_length(self):

        user = "example-test"
        roles = ["read" * 50]

        with pytest.raises(e.ClientError):
            self.client.admin_revoke_roles(user, roles)
