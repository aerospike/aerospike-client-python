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

class TestCreateRole(TestBaseClass):

    def setup_method(self, method):

        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {
                "hosts": hostlist
                }
        self.client = aerospike.client(config).connect( user, password )

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
        status = self.client.admin_create_role("usr-sys-admin", [{"code":
aerospike.READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")

        assert roles[0].get('role') == "usr-sys-admin"

        status = self.client.admin_create_user("testcreaterole", "createrole",
["usr-sys-admin"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == ["usr-sys-admin"]

        status = self.client.admin_drop_role("usr-sys-admin")

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_positive(self):
        """
            Create role positive
        """
        status = self.client.admin_create_role("usr-sys-admin", [{"code": aerospike.USER_ADMIN}, {"code": aerospike.SYS_ADMIN}])

        assert status == 0

        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")

        assert roles[0].get('role') == "usr-sys-admin"

        status = self.client.admin_create_user("testcreaterole", "createrole",
["usr-sys-admin"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == ["usr-sys-admin"]

        status = self.client.admin_drop_role("usr-sys-admin")

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_incorrect_role_type(self):
        """
            role name not string
        """
        with pytest.raises(Exception) as exception:
            self.client.admin_create_role(1, [{"code": aerospike.USER_ADMIN}])

        assert exception.value[0] == -2
        assert exception.value[1] == "Role name should be a string"

    def test_create_role_unknown_privilege_type(self):
        """
            privilege type unknown
        """
        with pytest.raises(AttributeError) as attributeError:
            self.client.admin_create_role("usr-sys-admin", [{"code": aerospike.USER_ADMIN_WRONG}])

        assert "'module' object has no attribute 'USER_ADMIN_WRONG'" in attributeError.value

    def test_create_role_incorrect_privilege_type(self):
        """
            privilege type incorrect
        """
        with pytest.raises(Exception) as exception:
            self.client.admin_create_role("usr-sys-admin", None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Privileges should be a list"

    def test_create_role_existing_role(self):
        """
            create an already existing role
        """
        status = self.client.admin_create_role("usr-sys-admin", [{"code": aerospike.USER_ADMIN}, {"code": aerospike.SYS_ADMIN}])

        assert status == 0

        with pytest.raises(Exception) as exception:
            self.client.admin_create_role("usr-sys-admin", [{"code": aerospike.USER_ADMIN}, {"code": aerospike.SYS_ADMIN}])

        assert exception.value[0] == 71
        assert exception.value[1] == "AEROSPIKE_ROLE_ALREADY_EXISTS"

        time.sleep(1)
        status = self.client.admin_drop_role("usr-sys-admin")

        assert status == 0

    def test_create_role_positive_with_special_characters(self):
        """
            Create role positive with special characters in role name
        """
        role_name = "!#Q#AEQ@#$%&^*((^&*~~~````"
        status = self.client.admin_create_role(role_name, [{"code":
aerospike.READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role(role_name)

        assert roles[0].get('role') == role_name

        status = self.client.admin_create_user("testcreaterole", "createrole",
[role_name])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == [role_name]

        status = self.client.admin_drop_role(role_name)

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users[0]['roles'] == []

        self.client.admin_drop_user("testcreaterole")

    def test_create_role_positive_with_too_long_role_name(self):
        """
            Create role positive with too long role name
        """
        role_name = "role$"*1000

        with pytest.raises(Exception) as exception:
            self.client.admin_create_role(role_name, [{"code":
aerospike.READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        assert exception.value[0] == 70
        assert exception.value[1] == "AEROSPIKE_INVALID_ROLE"

