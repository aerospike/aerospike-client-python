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


class TestSetWhitelist(TestBaseClass):

    config = TestBaseClass.get_connection_config()

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(),
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        config = self.config
        self.client = aerospike.client(config).connect(config['user'], config['password'])
        usr_sys_admin_privs = [
            {"code": aerospike.PRIV_USER_ADMIN},
            {"code": aerospike.PRIV_SYS_ADMIN}]
        whitelist = [
            "127.0.0.1"
        ]
        try:
            self.client.admin_drop_role("usr-sys-admin-test")
            time.sleep(2)
        except:
            pass
        self.client.admin_create_role(
            "usr-sys-admin-test", usr_sys_admin_privs, whitelist=whitelist)

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

    def test_admin_set_whitelist_no_parameters(self):
        """
        Set whitelist with no parameters.
        """
        with pytest.raises(TypeError):
            self.client.admin_set_whitelist()

    def test_admin_set_whitelist_empty_whitelist_positive(self):
        """
        Set whitelist with no whitelist. (will reset whitelist on a role with existing whitelist)
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test", whitelist=[])
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

    def test_admin_set_whitelist_none_whitelist_positive(self):
        """
        Set whitelist with no whitelist. (will reset whitelist on a role with existing whitelist)
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test", whitelist=None)
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

    def test_admin_set_whitelist_one_whitelist_positive(self):
        """
        Set whitelist with whitelist.
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test", whitelist=["10.0.2.0/24", "127.0.0.1", "127.0.0.2"])
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [
					"10.0.2.0/24",
					"127.0.0.1",
					"127.0.0.2"
				],
                'read_quota': 0,
                'write_quota': 0
            }

    def test_admin_set_quota_empty_positive(self):
        """
        Set whitelist positive
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test", whitelist=[])
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

    def test_admin_set_whitelist_positive_with_policy(self):
        """
        Set whitelist positive policy
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test",
										whitelist=["10.0.2.0/24", "127.0.0.1"],
										policy={'timeout': 1000})
        time.sleep(1)
        roles = self.client.admin_get_role("usr-sys-admin-test")
        assert roles == {
                'privileges': [
                    {'ns': '', 'set': '', 'code': 0},
                    {'ns': '', 'set': '', 'code': 1}
                ],
                'whitelist': [
					"10.0.2.0/24",
					"127.0.0.1"
				],
                'read_quota': 0,
                'write_quota': 0
            }

    def test_admin_set_whitelist_incorrect_role_name(self):
        """
        Incorrect role name
        """
        try:
            self.client.admin_set_whitelist(role="bad-role-name",
											whitelist=["10.0.2.0/24"],
											policy={'timeout': 1000})

        except e.InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_admin_set_whitelist_incorrect_role_type(self):
        """
        Incorrect role type
        """
        try:
            self.client.admin_set_whitelist(role=None,
											whitelist=["10.0.2.0/24"],
											policy={'timeout': 1000})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string."

    def test_admin_set_whitelist_incorrect_whitelist(self):
        """
        Incorrect role name
        """
        try:
            self.client.admin_set_whitelist(role="usr-sys-admin-test",
											whitelist=["bad_IP"],
											policy={'timeout': 1000})
        except e.InvalidWhitelist as exception:
            assert exception.code == 73
            assert exception.msg == "AEROSPIKE_INVALID_WHITELIST"

    def test_admin_set_whitelist_incorrect_whitelist_type(self):
        """
        Incorrect role type
        """
        try:
            self.client.admin_set_whitelist(role="usr-sys-admin-test",
                                                whitelist=None,
                                                policy={'timeout': 1000})

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Whitelist must be a list of IP strings."

    def test_admin_set_whitelist_forbiden_host(self):
        """
        Forbiden host
        """
        self.client.admin_set_whitelist(role="usr-sys-admin-test",
                                            whitelist=["123.4.5.6"],
                                            policy={'timeout': 1000})

        self.client.admin_create_user("test_whitelist_user", "123", ["usr-sys-admin-test"])

        config = TestBaseClass.get_connection_config()
        new_client = aerospike.client(config).connect(config['user'], config['password'])
        try:
            new_client.connect("test_whitelist_user", "123")
        except e.NotWhitelisted as exception:
            assert exception.code == 82
            assert exception.msg == "Failed to connect"
        finally:
            self.client.admin_drop_user("test_whitelist_user")