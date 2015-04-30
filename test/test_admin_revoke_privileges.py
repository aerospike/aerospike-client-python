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

class TestRevokePrivilege(TestBaseClass):

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

    def test_admin_revoke_privileges_no_parameters(self):
        """
            Revoke privileges with no parameters
        """
        with pytest.raises(TypeError) as typeError:
            self.client.admin_revoke_privileges()

        assert "Required argument 'role' (pos 1) not found" in typeError.value

    def test_admin_revoke_privileges_positive(self):
        """
            revoke privileges positive
        """
        status = self.client.admin_grant_privileges("usr-sys-admin", [{"code":
aerospike.READ}])

        assert status == 0

        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges("usr-sys-admin", [{"code": aerospike.READ}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_with_policy(self):
        """
            Revoke privileges positive with policy
        """
        status = self.client.admin_grant_privileges("usr-sys-admin", [{"code":
aerospike.READ}], {'timeout': 1000})

        assert status == 0

        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': '', 'set': ''}]

        status = self.client.admin_revoke_privileges("usr-sys-admin", [{"code": aerospike.READ}], {'timeout': 1000})
        time.sleep(1)
        assert status == 0
        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}]

    def test_admin_revoke_privileges_positive_with_ns_set(self):
        """
            Revoke privileges positive with ns and set
        """
        status = self.client.admin_grant_privileges("usr-sys-admin", [{"code":
aerospike.READ, "ns":"test", "set":"demo"}])

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}, {'code': 10, 'ns': 'test', 'set': 'demo'}]

        status = self.client.admin_revoke_privileges("usr-sys-admin", [{"code":
aerospike.READ, "ns":"test", "set":"demo"}])
        time.sleep(1)
        assert status == 0
        roles = self.client.admin_query_role("usr-sys-admin")
        assert roles[0]['privileges'] == [{'code': 0, 'ns': '', 'set': ''},
{'code': 1, 'ns': '', 'set': ''}]

    def test_revoke_privileges_incorrect_role_type(self):
        """
            role name not string
        """
        with pytest.raises(Exception) as exception:
            self.client.admin_revoke_privileges(1, [{"code": aerospike.USER_ADMIN}])

        assert exception.value[0] == -2
        assert exception.value[1] == "Role name should be a string"

    def test_revoke_privileges_unknown_privilege_type(self):
        """
            privilege type unknown
        """
        with pytest.raises(AttributeError) as attributeError:
            self.client.admin_revoke_privileges("usr-sys-admin", [{"code": aerospike.USER_ADMIN_WRONG}])

        assert "'module' object has no attribute 'USER_ADMIN_WRONG'" in attributeError.value

    def test_revoke_privileges_incorrect_privilege_type(self):
        """
            privilege type incorrect
        """
        with pytest.raises(Exception) as exception:
            self.client.admin_revoke_privileges("usr-sys-admin", None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Privileges should be a list"

    def test_revoke_privileges_empty_list_privileges(self):
        """
            privilege type is an empty list
        """
        with pytest.raises(Exception) as exception:
            self.client.admin_revoke_privileges("usr-sys-admin", [])

        assert exception.value[0] == 72
        assert exception.value[1] == 'AEROSPIKE_INVALID_PRIVILEGE'
