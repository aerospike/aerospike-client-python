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

class TestCreateRole(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass().get_hosts()[1] == None,
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):

        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = { "hosts": hostlist}
        self.client = aerospike.client(config).connect( user, password )
        try:
            self.client.admin_drop_user("testcreaterole")
        except:
            pass # do nothing, EAFP

        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """

        policy = {}

        for user in self.delete_users:
            self.client.admin_drop_user( user, policy )

        self.client.close()

    def test_create_role_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_create_role()

        assert "Required argument 'role' (pos 1) not found" in typeError.value

    def test_create_role_positive_with_policy(self):
        """
            Create role positive
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
        except InvalidRole:
            pass # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test",
                [{"code": aerospike.PRIV_READ, "ns": "test", "set":"demo"}],
                {'timeout': 1000})
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")
        assert roles == [{'code': 10, 'ns': 'test', 'set': 'demo'}]

        status = self.client.admin_create_user("testcreaterole", "createrole", ["usr-sys-admin-test"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users == ["usr-sys-admin-test"]

        status = self.client.admin_drop_role("usr-sys-admin-test")

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_positive(self):
        """
            Create role positive
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
        except InvalidRole:
            pass # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test", [{"code":
            aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}])
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin-test")

        assert roles == [{"code": 0, 'ns': '', 'set': ''}, {"code": 1, 'ns': '', 'set': ''}]

        status = self.client.admin_create_user("testcreaterole", "createrole",
["usr-sys-admin-test"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users == ["usr-sys-admin-test"]

        status = self.client.admin_drop_role("usr-sys-admin-test")

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_incorrect_role_type(self):
        """
            role name not string
        """
        try:
            self.client.admin_create_role(1, [{"code": aerospike.PRIV_USER_ADMIN}])
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_create_role_unknown_privilege_type(self):
        """
            privilege type unknown
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
        except InvalidRole:
            pass # we are good, no such role exists

        try:
            self.client.admin_create_role("usr-sys-admin-test", [{"code": 64}])
        except InvalidPrivilege as exception:
            assert exception.code == 72

    def test_create_role_incorrect_privilege_type(self):
        """
            privilege type incorrect
        """
        try:
            self.client.admin_create_role("usr-sys-admin-test", None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Privileges should be a list"

    def test_create_role_existing_role(self):
        """
            create an already existing role
        """
        try:
            self.client.admin_query_role("usr-sys-admin-test")
            # role exists, clear it out.
            self.client.admin_drop_role("usr-sys-admin-test")
        except InvalidRole:
            pass # we are good, no such role exists

        self.client.admin_create_role("usr-sys-admin-test", [{"code":
            aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}])
        try:
            self.client.admin_create_role("usr-sys-admin-test", [{"code":
                aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}])

        except RoleExistsError as exception:
            assert exception.code == 71
            assert exception.msg == "AEROSPIKE_ROLE_ALREADY_EXISTS"

        time.sleep(1)
        status = self.client.admin_drop_role("usr-sys-admin-test")

        assert status == 0

    def test_create_role_positive_with_special_characters(self):
        """
            Create role positive with special characters in role name
        """
        role_name = "!#Q#AEQ@#$%&^*((^&*~~~````"
        try:
            self.client.admin_drop_role(role_name) # clear out if it exists
        except:
            pass # EAFP
        status = self.client.admin_create_role(role_name, [{"code": aerospike.PRIV_READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role(role_name)

        assert roles == [{"code": aerospike.PRIV_READ, "ns": "test", "set": "demo"}]

        status = self.client.admin_create_user("testcreaterole", "createrole", [role_name])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users == [role_name]

        status = self.client.admin_drop_role(role_name)

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_positive_with_too_long_role_name(self):
        """
            Create role positive with too long role name
        """
        role_name = "role$"*1000

        try:
            self.client.admin_create_role(role_name, [{"code":
aerospike.PRIV_READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        except InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

