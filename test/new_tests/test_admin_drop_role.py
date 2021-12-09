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
class TestDropRole(object):

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
                time.sleep(2)
            except:
                pass

        self.client.close()

    def test_drop_role_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_role()

        assert "argument 'role' (pos 1)" in str(
            typeError.value)

    def test_drop_role_positive_with_policy(self):
        """
            Drop role positive with policy
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(1)
        except e.InvalidRole:
            pass  # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test",
                                      [{"code": aerospike.PRIV_READ,
                                        "ns": "test", "set": "demo"}],
                                      {'timeout': 1000})
        time.sleep(1)

        status = self.client.admin_drop_role(
            "usr-sys-admin-test", {'timeout': 1000})

        assert status == 0
        time.sleep(1)

        with pytest.raises(e.InvalidRole):
            self.client.admin_query_role("usr-sys-admin-test")

    def test_drop_role_positive_with_policy_write(self):
        """
            Drop write role positive with policy
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(1)
        except e.InvalidRole:
            pass  # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test",
                                      [{"code": aerospike.PRIV_WRITE,
                                        "ns": "test", "set": "demo"}],
                                      {'timeout': 1000})
        time.sleep(1)

        status = self.client.admin_drop_role(
            "usr-sys-admin-test", {'timeout': 1000})

        assert status == 0
        time.sleep(1)

        with pytest.raises(e.InvalidRole):
            self.client.admin_query_role("usr-sys-admin-test")

    def test_drop_role_positive(self):
        """
            Drop role positive
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(1)

        except e.InvalidRole:
            pass  # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test",
                                      [{"code": aerospike.PRIV_USER_ADMIN},
                                       {"code": aerospike.PRIV_SYS_ADMIN}])
        time.sleep(1)
        privs = self.client.admin_query_role("usr-sys-admin-test")
        assert privs == [
            {"code": 0, "ns": "", "set": ""}, {"code": 1, "ns": "", "set": ""}]

        self.client.admin_drop_role("usr-sys-admin-test")
        time.sleep(1)

        with pytest.raises(e.InvalidRole):
            self.client.admin_query_role("usr-sys-admin-test")

    def test_drop_non_existent_role(self):
        """
            Drop non-existent role
        """
        try:
            self.client.admin_drop_role("usr-sys-admin-test")

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_drop_role_rolename_None(self):
        """
            Drop role with role name None
        """
        try:
            self.client.admin_drop_role(None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_drop_role_with_incorrect_policy(self):
        """
            Drop role with incorrect policy
        """
        status = self.client.admin_create_role(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_USER_ADMIN}])

        assert status == 0
        time.sleep(3)
        try:
            self.client.admin_drop_role("usr-sys-admin-test", {"timeout": 0.2})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
