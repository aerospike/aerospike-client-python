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


class TestGetRole(TestBaseClass):

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
            self.client.admin_drop_role("usr-sys-admin")
            time.sleep(2)
        except:
            pass
        usr_sys_admin_privs = [
            {"code": aerospike.PRIV_USER_ADMIN},
            {"code": aerospike.PRIV_SYS_ADMIN}]
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(2)
        except:
            pass
        self.client.admin_create_role(
            "usr-sys-admin-test", usr_sys_admin_privs)

        self.delete_users = []
        time.sleep(1)

    def teardown_method(self, method):
        """
        Teardown method
        """
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
        self.client.close()

    def test_admin_get_role_no_parameters(self):
        """
        Get role with no parameters
        """
        with pytest.raises(TypeError):
            self.client.admin_get_role()

    def test_admin_get_role_positive(self):
        """
            Get role positive
        """
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 0,
                'write_quota': 0
            }

    def test_admin_get_role_positive_with_policy(self):
        """
        Get role positive policy
        """
        roles = self.client.admin_get_role(
            "usr-sys-admin-test", {'timeout': 1000})
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 0,
                'write_quota': 0
            }

    def test_admin_get_role_incorrect_role_name(self):
        """
        Incorrect role name
        """
        try:
            self.client.admin_get_role(
                "usr-sys-admin-test-non-existent", {'timeout': 1000})

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_admin_get_role_incorrect_role_type(self):
        """
        Incorrect role type
        """
        try:
            self.client.admin_get_role(None, {'timeout': 1000})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"
