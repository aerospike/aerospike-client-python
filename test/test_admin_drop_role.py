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

class TestDropRole(TestBaseClass):

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

        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """

        policy = {}

        for user in self.delete_users:
            self.client.admin_drop_user( user, policy )

        self.client.close()

    def test_drop_role_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            self.client.admin_drop_role()

        assert "Required argument 'role' (pos 1) not found" in typeError.value

    def test_drop_role_positive_with_policy(self):
        """
            Drop role positive with policy
        """
        status = self.client.admin_create_role("usr-sys-admin", [{"code":
aerospike.PRIV_READ, "ns": "test", "set":"demo"}], {'timeout': 1000})

        assert status == 0
        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")

        assert roles == [{"code": 10, "ns": "test", "set": "demo"}]

        status = self.client.admin_create_user("testcreaterole", "createrole",
["usr-sys-admin"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users == ["usr-sys-admin"]

        status = self.client.admin_drop_role("usr-sys-admin", {'timeout': 1000})

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users == []

        self.client.admin_drop_user("testcreaterole")

    def test_drop_role_positive(self):
        """
            Drop role positive
        """
        status = self.client.admin_create_role("usr-sys-admin", [{"code":
            aerospike.PRIV_USER_ADMIN}, {"code": aerospike.PRIV_SYS_ADMIN}])

        assert status == 0

        time.sleep(1)
        roles = self.client.admin_query_role("usr-sys-admin")

        assert roles == [{"code": 0, "ns": "", "set": ""}, {"code": 1, "ns": "", "set": ""}]

        status = self.client.admin_create_user("testcreaterole", "createrole",
["usr-sys-admin"])

        assert status == 0
        time.sleep(1)
        users = self.client.admin_query_user("testcreaterole")

        assert users == ["usr-sys-admin"]

        status = self.client.admin_drop_role("usr-sys-admin")

        assert status == 0

        users = self.client.admin_query_user("testcreaterole")

        assert users == []

        self.client.admin_drop_user("testcreaterole")

    def test_drop_non_existent_role(self):
        """
            Drop non-existent role
        """
        try:
            self.client.admin_drop_role("usr-sys-admin")

        except InvalidRole as exception:
            assert exception.code == 70
            assert exception.msg == "AEROSPIKE_INVALID_ROLE"

    def test_drop_role_rolename_None(self):
        """
            Drop role with role name None
        """
        try:
            self.client.admin_drop_role(None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Role name should be a string"

    def test_drop_role_with_incorrect_policy(self):
        """
            Drop role with incorrect policy
        """
        status = self.client.admin_create_role("usr-sys-admin", [{"code":
            aerospike.PRIV_USER_ADMIN}])

        assert status == 0

        try:
            self.client.admin_drop_role("usr-sys-admin", {"timeout": 0.2})

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'timeout is invalid'

        self.client.admin_drop_role("usr-sys-admin")
