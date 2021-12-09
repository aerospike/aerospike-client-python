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
class TestDropUser(object):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method.
        """
        config = TestBaseClass.get_connection_config()
        TestDropUser.Me = self
        self.client = aerospike.client(config).connect(config['user'], config['password'])
        try:
            self.client.admin_drop_user("foo-test")
            time.sleep(2)
        except:
            pass

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

        assert "argument 'user' (pos 1)" in str(
            typeError.value)

    def test_drop_user_with_policy_none(self):
        """
            Invoke drop_user() with policy none
        """
        policy = None
        user = "foo-test"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(2)

        assert status == 0
        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']

        status = self.client.admin_drop_user(user, policy)

        assert status == 0

        try:
            user_details = self.client.admin_query_user(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == 'AEROSPIKE_INVALID_USER'

    def test_drop_user_with_user_none(self):
        """
            Invoke drop_user() with policy none
        """
        policy = {'timeout': 1000}
        try:
            self.client.admin_drop_user(None, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Username should be a string'

    def test_drop_user_positive(self):
        """
            Invoke drop_user() with correct arguments.
        """
        policy = {'timeout': 1000}
        user = "foo-test"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(1)

        assert status == 0
        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']
        status = self.client.admin_drop_user(user, policy)
        assert status == 0

        time.sleep(2)

        try:
            user_details = self.client.admin_query_user(user, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == 'AEROSPIKE_INVALID_USER'

    def test_drop_user_positive_without_policy(self):
        """
            Invoke drop_user() with correct arguments.
        """
        policy = {
            'timeout': 1000
        }
        user = "foo-test"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(1)

        assert status == 0
        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']
        status = self.client.admin_drop_user(user)
        assert status == 0

        time.sleep(1)

        try:
            user_details = self.client.admin_query_user(user, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == 'AEROSPIKE_INVALID_USER'

    def test_drop_user_negative(self):
        """
            Invoke drop_user() with non-existent user.
        """
        policy = {}
        user = "foo-test"
        try:
            self.client.admin_query_user(user, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == 'AEROSPIKE_INVALID_USER'

        try:
            self.client.admin_drop_user(user)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == 'AEROSPIKE_INVALID_USER'

    def test_drop_user_policy_incorrect(self):
        """
            Invoke drop_user() with policy incorrect
        """
        policy = {'timeout': 1000}
        user = "incorrect-policy"
        password = "foo1"
        roles = ["read", "read-write", "sys-admin"]

        status = self.client.admin_create_user(user, password, roles, policy)

        time.sleep(1)

        assert status == 0
        user_details = self.client.admin_query_user(user, policy)

        assert user_details == ['read', 'read-write', 'sys-admin']
        policy = {
            'timeout': 0.2
        }
        try:
            status = self.client.admin_drop_user(user, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'

        status = self.client.admin_drop_user(user)

    def test_drop_user_with_extra_argument(self):
        """
            Invoke drop_user() with extra argument.
        """
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_user("foo-test", policy, "")

        assert "admin_drop_user() takes at most 2 arguments (3 given)" in str(
            typeError.value)

    @pytest.mark.xfail(reason="It is no longer possible to create a user with"
                       "a name too long")
    def test_drop_user_with_too_long_username(self):

        policy = {}
        user = "user$" * 1000
        password = "user10"
        roles = ["sys-admin"]

        try:
            self.client.admin_create_user(user, password, roles, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

        try:
            self.client.admin_drop_user(user, policy)

        except e.InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_drop_user_with_special_characters_in_username(self):

        policy = {}
        user = "!#Q#AEQ@#$%&^*((^&*~~~````"
        password = "user4"
        roles = ["read-write"]

        try:
            status = self.client.admin_create_user(
                user, password, roles, policy)
            assert status == 0
            time.sleep(1)
        except:
            pass

        status = self.client.admin_drop_user(user)

        assert status == 0
