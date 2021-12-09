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


class TestSetPassword(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        TestSetPassword.Me = self
        self.client = aerospike.client(config).connect(config['user'], config['password'])
        try:
            self.client.admin_drop_user("testsetpassworduser")
            time.sleep(2)
        except e.InvalidUser:
            pass

        try:
            self.client.admin_create_user(
                "testsetpassworduser", "aerospike", ["read"], {})
        except e.UserExistsError:
            pass

        time.sleep(2)
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        try:
            self.client.admin_drop_user("testsetpassworduser")
            time.sleep(2)
        except e.InvalidUser:
            pass
        self.client.close()

    def test_set_password_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_set_password()

        assert "argument 'user' (pos 1)" in str(
            typeError.value)

    def test_set_password_with_proper_parameters(self):

        user = "testsetpassworduser"
        password = "newpassword"

        status = self.client.admin_set_password(user, password)

        assert status == 0

    def test_set_password_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}
        user = "testsetpassworduser"
        password = "newpassword"

        try:
            self.client.admin_set_password(user, password, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_set_password_with_proper_timeout_policy_value(self):

        policy = {'timeout': 50}
        user = "testsetpassworduser"
        password = "newpassword"

        status = self.client.admin_set_password(user, password, policy)

        assert status == 0

    def test_set_password_with_none_username(self):

        user = None
        password = "newpassword"

        try:
            self.client.admin_set_password(user, password)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_set_password_with_none_password(self):

        user = "testsetpassworduser"
        password = None

        try:
            self.client.admin_set_password(user, password)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_set_password_with_non_existent_user(self):

        policy = {}
        user = "new_user"
        password = "newpassword"

        try:
            self.client.admin_set_password(user, password, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_set_password_with_too_long_password(self):

        policy = {}
        user = "testsetpassworduser"
        password = "newpassword$" * 1000

        with pytest.raises(e.ClientError):
            self.client.admin_set_password(user, password, policy)
