# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestGrantPrivileges(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass().get_hosts()[1] == None,
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):

        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {
                "hosts": hostlist
                }
        self.client = aerospike.client(config).connect( user, password )

        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
        self.client.admin_create_role("usr-sys-admin-test", [{"code":
            aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}])
        self.delete_users = []
        time.sleep(1)
    def teardown_method(self, method):

        """
        Teardown method
        """

        policy = {}

        self.client.admin_drop_role("usr-sys-admin-test")
        self.client.close()

    def test_admin_grant_privileges_no_parameters(self):
        """
            Grant privileges with no parameters
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_grant_privileges()

        assert "Required argument 'role' (pos 1) not found" in typeError.value

    def test_admin_grant_privileges_positive(self):
        """
            Grant privileges positive
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", [{"code":
aerospike.PRIV_READ}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles== [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code":
            aerospike.PRIV_READ}])

        assert status == 0

    def test_admin_grant_privileges_positive_with_policy(self):
        """
            Grant privileges positive with policy
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", [{"code":
aerospike.PRIV_READ}], {'timeout': 1000})

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code":
            aerospike.PRIV_READ}])

        assert status == 0

    def test_admin_grant_privileges_positive_with_ns_set(self):
        """
            Grant privileges positive with ns and set
        """
        status = self.client.admin_grant_privileges("usr-sys-admin-test", [{"code":
aerospike.PRIV_READ, "ns":"test", "set":"demo"}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': 'test', 'set': 'demo'}]

        status = self.client.admin_revoke_privileges("usr-sys-admin-test", [{"code":
aerospike.PRIV_READ, "ns":"test", "set":"demo"}])

        assert status == 0

    def test_grant_privileges_incorrect_role_type(self):
        """
            role name not string
        """
        try:
            self.client.admin_grant_privileges(1, [{"code": aerospike.PRIV_USER_ADMIN}])

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_grant_privileges_unknown_privilege_type(self):
        """
            privilege type unknown
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", [{"code": 64}])
        except InvalidPrivilege as exception:
            assert exception.code == 72

    def test_grant_privileges_incorrect_privilege_type(self):
        """
            privilege type incorrect
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Privileges should be a list"

    def test_grant_privileges_empty_list_privileges(self):
        """
            privilege type is an empty list
        """
        try:
            self.client.admin_grant_privileges("usr-sys-admin-test", [])

        except InvalidPrivilege as exception:
            assert exception.code == 72
            assert exception.msg == 'AEROSPIKE_INVALID_PRIVILEGE'
