# -*- coding: utf-8 -*-

import pytest
import time
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike
from .conftest import admin_drop_role_and_poll, poll_until_role_doesnt_exist, admin_create_role_and_poll


class TestQueryRoles(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    def setup_method(self, method):
        """
        Setup method
        """
        config = TestBaseClass.get_connection_config()
        self.client = aerospike.client(config).connect(config["user"], config["password"])
        try:
            admin_drop_role_and_poll("usr-sys-admin")
        except Exception:
            pass
        usr_sys_admin_privs = [{"code": aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}]
        try:
            admin_drop_role_and_poll("usr-sys-admin-test")
        except Exception:
            pass
        admin_create_role_and_poll(self.client, "usr-sys-admin-test", usr_sys_admin_privs)
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """
        try:
            admin_drop_role_and_poll("usr-sys-admin-test")
        except Exception:
            pass
        self.client.close()

    def test_admin_query_roles_positive(self):
        """
        Query roles positive
        """
        roles = self.client.admin_query_roles()

        assert roles["usr-sys-admin-test"] == [{"code": 0, "ns": "", "set": ""}, {"code": 1, "ns": "", "set": ""}]

    def test_admin_query_roles_positive_with_policy(self):
        """
        Query roles positive policy
        """
        roles = self.client.admin_query_roles({"timeout": 180000})

        assert roles["usr-sys-admin-test"] == [{"code": 0, "ns": "", "set": ""}, {"code": 1, "ns": "", "set": ""}]

    def test_admin_query_roles_incorrect_policy(self):
        """
        Query roles incorrect policy
        """
        try:
            self.client.admin_query_roles({"timeout": 0.2})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"
