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


class TestQueryRoles(TestBaseClass):

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
        except:
            pass
        time.sleep(2)
        usr_sys_admin_privs = [
            {"code": aerospike.PRIV_USER_ADMIN},
            {"code": aerospike.PRIV_SYS_ADMIN}]
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
        time.sleep(2)        
        self.client.admin_create_role(
            "usr-sys-admin-test", usr_sys_admin_privs)
        self.delete_users = []
        time.sleep(2)

    def teardown_method(self, method):
        """
        Teardown method
        """
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
        except:
            pass
        self.client.close()

    def test_admin_query_roles_positive(self):
        """
            Query roles positive
        """
        roles = self.client.admin_query_roles()

        assert roles[
            'usr-sys-admin-test'] == [{'code': 0, 'ns': '', 'set': ''},
                                      {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_query_roles_positive_with_policy(self):
        """
            Query roles positive policy
        """
        roles = self.client.admin_query_roles({'timeout': 1000})

        assert roles[
            'usr-sys-admin-test'] == [{'code': 0, 'ns': '', 'set': ''},
                                      {'code': 1, 'ns': '', 'set': ''}]

    def test_admin_query_roles_incorrect_policy(self):
        """
            Query roles incorrect policy
        """
        try:
            self.client.admin_query_roles({'timeout': 0.2})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'
