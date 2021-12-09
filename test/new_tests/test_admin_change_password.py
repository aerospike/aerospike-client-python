# -*- coding: utf-8 -*-

import pytest
import sys
import time
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("connection_config")
class TestChangePassword(object):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
            Setup method
            """
        config = TestBaseClass.get_connection_config()
        self.client = aerospike.client(config).connect(config['user'], config['password'])

        try:
            self.client.admin_create_user(
                "testchangepassworduser", "aerospike", ["read"], {})
            time.sleep(2)
        except aerospike.exception.UserExistsError:
            pass  # we are good, no such role exists
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        try:
            self.client.admin_drop_user("testchangepassworduser")
        except:
            pass

        self.client.close()

    def test_change_password_without_any_parameters(self):

        with pytest.raises(TypeError):
            self.client.admin_change_password()

    # NOTE: This will fail if auth_mode is PKI_AUTH (3).
    @pytest.mark.xfail(reason="Might fail depending on auth_mode.")
    def test_change_password_with_proper_parameters(self):

        user = "testchangepassworduser"
        config = self.connection_config
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        password = "newpassword"

        status = self.clientreaduser.admin_change_password(user, password)


        assert status == 0

        time.sleep(2)
        config = self.connection_config

        # Assert that connecting to the server with the old password fails
        with pytest.raises(
            (aerospike.exception.InvalidPassword,
                aerospike.exception.InvalidCredential)):
            self.clientreaduserwrong = aerospike.client(
                config).connect(user, "aerospike")

        self.clientreaduserright = aerospike.client(config).connect(
            user, "newpassword")

        assert self.clientreaduserright is not None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}
        user = "testchangepassworduser"
        password = "newpassword"

        try:
            self.client.admin_change_password(user, password, policy)

        except aerospike.exception.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    # NOTE: This will fail if auth_mode is PKI_AUTH (3).
    @pytest.mark.xfail(reason="Might fail depending on auth_mode.")
    def test_change_password_with_proper_timeout_policy_value(self):

        user = "testchangepassworduser"
        config = self.connection_config
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        policy = {'timeout': 1000}
        password = "newpassword"

        status = self.clientreaduser.admin_change_password(
            user, password, policy)

        assert status == 0
        time.sleep(2)
        config = self.connection_config

        with pytest.raises(
            (aerospike.exception.InvalidPassword,
                aerospike.exception.InvalidCredential)):
            self.clientreaduserwrong = aerospike.client(
                config).connect(user, "aerospike")

        self.clientreaduserright = aerospike.client(config).connect(
            user, "newpassword")

        assert self.clientreaduserright is not None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_none_username(self):

        policy = {}
        user = None
        password = "newpassword"

        try:
            self.client.admin_change_password(user, password, policy)

        except aerospike.exception.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_change_password_with_none_password(self):

        policy = {}
        user = "testchangepassworduser"
        password = None

        try:
            self.client.admin_change_password(user, password, policy)

        except aerospike.exception.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_change_password_with_non_existent_user(self):

        policy = {}
        user = "readwriteuser"
        password = "newpassword"

        try:
            self.client.admin_change_password(user, password, policy)

        except aerospike.exception.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_change_password_with_too_long_password(self):

        user = "testchangepassworduser"
        config = self.connection_config
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        policy = {'timeout': 100}
        password = "password" * 1000

        with pytest.raises(aerospike.exception.ClientError):
            status = self.clientreaduser.admin_change_password(
                user, password, policy)

        self.clientreaduser.close()
