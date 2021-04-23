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


class TestSetQuotas(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = TestBaseClass.get_connection_config()
        self.client = aerospike.client(config).connect(user, password)
        usr_sys_admin_privs = [
            {"code": aerospike.PRIV_USER_ADMIN},
            {"code": aerospike.PRIV_SYS_ADMIN}]
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(2)
        except:
            pass
        self.client.admin_create_role(
            "usr-sys-admin-test", usr_sys_admin_privs, write_quota=4500)

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

    def test_admin_set_quota_no_parameters(self):
        """
        Set quotas with no parameters.
        """
        with pytest.raises(TypeError):
            self.client.admin_set_quotas()

    def test_admin_set_quota_no_quotas_positive(self):
        """
        Set quotas with no quotas. (will reset quotas on a role with existing quotas)
        """
        self.client.admin_set_quotas(role="usr-sys-admin-test",)
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 0,
                'write_quota': 4500
            }

    def test_admin_set_quota_one_quota_positive(self):
        """
        Set quotas with one quota.
        """
        self.client.admin_set_quotas(role="usr-sys-admin-test", read_quota=250)
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 250,
                'write_quota': 4500
            }

    def test_admin_set_quota_positive(self):
        """
        Set Quota positive
        """
        self.client.admin_set_quotas(role="usr-sys-admin-test", read_quota=250, write_quota=300)
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 250,
                'write_quota': 300
            }

    def test_admin_set_quota_positive_reset(self):
        """
        Set Quota positive
        """
        self.client.admin_set_quotas(role="usr-sys-admin-test", read_quota=0, write_quota=0)
        time.sleep(1)
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

    def test_admin_set_quota_positive_with_policy(self):
        """
        Set Quota positive policy
        """
        self.client.admin_set_quotas(role="usr-sys-admin-test",
										read_quota=250,
										write_quota=300,
										policy={'timeout': 1000})
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [],
                'read_quota': 250,
                'write_quota': 300
            }

    def test_admin_set_quota_incorrect_role_name(self):
        """
        Incorrect role name
        """
        try:
            self.client.admin_set_quotas(role="bad-role-name",
											read_quota=250,
											write_quota=300,
											policy={'timeout': 1000})

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_admin_set_quota_incorrect_role_type(self):
        """
        Incorrect role type
        """
        try:
            self.client.admin_set_quotas(role=None,
											read_quota=250,
											write_quota=300,
											policy={'timeout': 1000})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_admin_set_quota_incorrect_quota(self):
        """
        Incorrect role name
        """
        try:
            self.client.admin_set_quotas(role="usr-sys-admin-test",
											read_quota=-20,
											write_quota=300,
											policy={'timeout': 1000})

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_admin_set_quota_incorrect_quota_type(self):
        """
        Incorrect role type
        """
        try:
            self.client.admin_set_quotas(role="usr-sys-admin-test",
											read_quota=None,
											write_quota=300,
											policy={'timeout': 1000})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Read_quota must be an integer."
