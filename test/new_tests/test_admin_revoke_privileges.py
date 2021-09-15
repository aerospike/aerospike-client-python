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


class TestRevokePrivilege(TestBaseClass):

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
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(2)
        except e.InvalidRole:
            pass
        self.client.admin_create_role("usr-sys-admin-test",
                                      [{"code": aerospike.PRIV_USER_ADMIN},
                                       {"code": aerospike.PRIV_SYS_ADMIN}])
        time.sleep(2)
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except e.InvalidRole:
            pass
        self.client.close()

    def test_admin_revoke_privileges_no_parameters(self):
        """
            Revoke privileges with no parameters
        """
        with pytest.raises(TypeError):
            self.client.admin_revoke_privileges()

    def test_admin_revoke_privileges_positive(self):
        """
            revoke privileges positive
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ}])

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''},
                         {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ}])

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_write(self):
        """
            revoke write privileges positive
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_WRITE}])

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''},
                         {'code': 13, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_WRITE}])

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_with_policy(self):
        """
            Revoke privileges positive with policy
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ}], {'timeout': 1000})

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''},
                         {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ}], {'timeout': 1000})

        time.sleep(1)
        assert status == 0
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_with_policy_write(self):
        """
            Revoke write privileges positive with policy
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_WRITE}], {'timeout': 1000})

        assert status == 0
        time.sleep(2)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''},
                         {'code': 13, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_WRITE}], {'timeout': 1000})

        time.sleep(1)
        assert status == 0
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_with_ns_set(self):
        """
            Revoke privileges positive with ns and set
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''},
                         {'code': 10, 'ns': 'test', 'set': 'demo'}]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test",
            [{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}])

        time.sleep(1)
        assert status == 0
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
                         {'code': 1, 'ns': '', 'set': ''}]

    def test_revoke_privileges_incorrect_role_type(self):
        """
            role name not string
        """
        try:
            self.client.admin_revoke_privileges(
                1, [{"code": aerospike.PRIV_USER_ADMIN}])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_revoke_privileges_unknown_privilege_type(self):
        """
            privilege type unknown
        """
        try:
            self.client.admin_revoke_privileges(
                "usr-sys-admin-test", [{"code": 64}])
        except e.InvalidPrivilege as exception:
            assert exception.code == 72

    def test_revoke_privileges_incorrect_privilege_type(self):
        """
            privilege type incorrect
        """
        try:
            self.client.admin_revoke_privileges("usr-sys-admin-test", None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Privileges should be a list"

    def test_revoke_privileges_empty_list_privileges(self):
        """
            privilege type is an empty list
        """
        try:
            self.client.admin_revoke_privileges("usr-sys-admin-test", [])

        except e.InvalidPrivilege as exception:
            assert exception.code == 72
            assert exception.msg == 'AEROSPIKE_INVALID_PRIVILEGE'
