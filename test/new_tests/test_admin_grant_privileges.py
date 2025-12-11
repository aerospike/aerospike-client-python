# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


class TestGrantPrivileges(object):

    config = TestBaseClass.get_connection_config()

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = self.config
        self.client = aerospike.client(config).connect(config["user"], config["password"])

        try:
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(1)
        except e.InvalidRole:
            pass
        self.client.admin_create_role(
            "usr-sys-admin-test", [{"code": aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}]
        )
        self.delete_users = []
        time.sleep(1)

    def teardown_method(self, method):
        """
        Teardown method
        """

        try:
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(1)
        except e.InvalidRole:
            pass
        self.client.close()

    def test_admin_grant_privileges_no_parameters(self):
        """
        Grant privileges with no parameters
        """
        with pytest.raises(TypeError):
            self.client.admin_grant_privileges()

    def test_admin_grant_privileges_positive(self):
        """
        Grant privileges positive
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", [{"code": aerospike.PRIV_READ}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [
            {"code": 0, "ns": "", "set": ""},
            {"code": 1, "ns": "", "set": ""},
            {"code": 10, "ns": "", "set": ""},
        ]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code": aerospike.PRIV_READ}])

        assert status == 0

    @pytest.mark.parametrize(
        "privs",
        [
            ([{"code": aerospike.PRIV_DATA_ADMIN, "ns": "", "set": ""}]),
            ([{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}]),
            ([{"code": aerospike.PRIV_WRITE, "ns": "test", "set": "demo"}]),
            ([{"code": aerospike.PRIV_READ_WRITE, "ns": "test", "set": "demo"}]),
            ([{"code": aerospike.PRIV_READ_WRITE_UDF, "ns": "test", "set": "demo"}]),
            ([{"code": aerospike.PRIV_TRUNCATE, "ns": "test", "set": "demo"}]),
            ([{"code": aerospike.PRIV_UDF_ADMIN, "ns": "", "set": ""}]),
            ([{"code": aerospike.PRIV_SINDEX_ADMIN, "ns": "", "set": ""}]),
        ],
    )
    def test_admin_grant_privileges_all_positive(self, privs):
        """
        Grant privileges positive
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", privs)

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{"code": 0, "ns": "", "set": ""}, {"code": 1, "ns": "", "set": ""}, *privs]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", privs)

        assert status == 0

    def test_admin_grant_privileges_positive_write(self):
        """
        Grant write privileges positive
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", [{"code": aerospike.PRIV_WRITE}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [
            {"code": 0, "ns": "", "set": ""},
            {"code": 1, "ns": "", "set": ""},
            {"code": 13, "ns": "", "set": ""},
        ]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code": aerospike.PRIV_WRITE}])

        assert status == 0

    def test_admin_grant_privileges_positive_with_policy(self):
        """
        Grant privileges positive with policy
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test", [{"code": aerospike.PRIV_READ}], {"timeout": 180000}
        )

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [
            {"code": 0, "ns": "", "set": ""},
            {"code": 1, "ns": "", "set": ""},
            {"code": 10, "ns": "", "set": ""},
        ]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code": aerospike.PRIV_READ}])

        assert status == 0

    def test_admin_grant_privileges_positive_with_ns_set(self):
        """
        Grant privileges positive with ns and set
        """
        status = self.client.admin_grant_privileges(
            "usr-sys-admin-test", [{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}]
        )

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [
            {"code": 0, "ns": "", "set": ""},
            {"code": 1, "ns": "", "set": ""},
            {"code": 10, "ns": "test", "set": "demo"},
        ]

        status = self.client.admin_revoke_privileges(
            "usr-sys-admin-test", [{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}]
        )

        assert status == 0

    def test_grant_privileges_incorrect_role_type(self):
        """
        role name not string
        """
        try:
            self.client.admin_grant_privileges(1, [{"code": aerospike.PRIV_USER_ADMIN}])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_grant_privileges_unknown_privilege_type(self):
        """
        privilege type unknown
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", [{"code": 64}])
        except e.InvalidPrivilege as exception:
            assert exception.code == 72

    def test_grant_privileges_incorrect_privilege_type(self):
        """
        privilege type incorrect
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", None)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Privileges should be a list"

    def test_grant_privileges_empty_list_privileges(self):
        """
        privilege type is an empty list
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", [])

        except e.InvalidPrivilege as exception:
            assert exception.code == 72
            assert exception.msg == "AEROSPIKE_INVALID_PRIVILEGE"
