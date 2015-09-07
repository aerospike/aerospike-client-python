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

class TestQueryRoles(TestBaseClass):

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
            self.client.admin_drop_role("usr-sys-admin")
        except:
            pass
        usr_sys_admin_privs =  [
            {"code": aerospike.PRIV_USER_ADMIN},
            {"code": aerospike.PRIV_SYS_ADMIN}]
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
        self.client.admin_create_role("usr-sys-admin-test", usr_sys_admin_privs)
        self.delete_users = []
        time.sleep(1)

    def teardown_method(self, method):

        """
        Teardown method
        """
        policy = {}

        self.client.admin_drop_role("usr-sys-admin-test")
        self.client.close()

    def test_admin_query_roles_positive(self):
        """
            Query roles positive
        """
        roles = self.client.admin_query_roles()

        flag = 0
        assert roles['usr-sys-admin-test'] == [{'code': 0, 'ns': '', 'set': ''}, {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_query_roles_positive_with_policy(self):
        """
            Query roles positive policy
        """
        roles = self.client.admin_query_roles({'timeout': 1000})

        flag = 0
        assert roles['usr-sys-admin-test'] == [{'code': 0, 'ns': '', 'set': ''}, {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_query_roles_incorrect_policy(self):
        """
            Query roles incorrect policy
        """
        try:
            roles = self.client.admin_query_roles({'timeout': 0.2})

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'
