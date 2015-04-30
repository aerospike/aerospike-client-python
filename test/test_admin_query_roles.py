# -*- coding: utf-8 -*-

import pytest
import sys
import time
from test_base_class import TestBaseClass

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestQueryRoles(TestBaseClass):

    def setup_method(self, method):

        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {
                "hosts": hostlist
                }
        self.client = aerospike.client(config).connect( user, password )

        self.client.admin_create_role("usr-sys-admin", [{"code": aerospike.USER_ADMIN}, {"code": aerospike.SYS_ADMIN}])
        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """
        policy = {}

        self.client.admin_drop_role("usr-sys-admin")
        self.client.close()

    def test_admin_query_roles_positive(self):
        """
            Query roles positive
        """
        roles = self.client.admin_query_roles()

        flag = 0
        for role in roles:
            if role['role'] == "usr-sys-admin":
                flag = 1
                assert role['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}]

        if not flag:
            assert True == False

    def test_admin_query_roles_positive_with_policy(self):
        """
            Query roles positive policy
        """
        roles = self.client.admin_query_roles({'timeout': 1000})

        flag = 0
        for role in roles:
            if role['role'] == "usr-sys-admin":
                flag = 1
                assert role['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}]

        if not flag:
            assert True == False

    def test_admin_query_roles_incorrect_policy(self):
        """
            Query roles incorrect policy
        """
        with pytest.raises(Exception) as exception:
            roles = self.client.admin_query_roles({'timeout': 0.2})

        assert exception.value[0] == -2
        assert exception.value[1] == 'timeout is invalid'
