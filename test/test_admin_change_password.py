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

class TestChangePassword(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass().get_hosts()[1] == None,
        reason="No user specified, may be not secured cluster.")

    def setup_method(self, method):
        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {"hosts": hostlist}
        self.client = aerospike.client(config).connect(user, password)

        try:
            self.client.admin_create_user( "testchangepassworduser", "aerospike", ["read"], {})
            time.sleep(2)
        except UserExistsError:
            pass # we are good, no such role exists
        self.delete_users = []

    def teardown_method(self, method):
        """
        Teardown method
        """

        self.client.admin_drop_user( "testchangepassworduser" )

        self.client.close()

    def test_change_password_without_any_parameters(self):

        with pytest.raises(TypeError) as typeError:
            status = self.client.admin_change_password()

        assert "Required argument 'user' (pos 1) not found" in typeError.value

    def test_change_password_with_proper_parameters(self):

        user = "testchangepassworduser"
        config = {"hosts": TestChangePassword.hostlist}
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        policy = {}
        password = "newpassword"

        status = self.clientreaduser.admin_change_password( user, password )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }
        try:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        except InvalidPassword as exception:
            assert exception.code == 62 
            assert exception.msg == None
        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Failed to seed cluster"

        self.clientreaduserright = aerospike.client(config).connect(
            user, "newpassword")

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_invalid_timeout_policy_value(self):

        policy = {'timeout': 0.1}
        user = "testchangepassworduser"
        password = "newpassword"

        try:
            status = self.client.admin_change_password( user, password, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_change_password_with_proper_timeout_policy_value(self):

        user = "testchangepassworduser"
        config = {"hosts": TestChangePassword.hostlist}
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        policy = {'timeout': 100}
        password = "newpassword"

        status = self.clientreaduser.admin_change_password( user, password, policy )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }

        try:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        except InvalidPassword as exception:
            assert exception.code == 62 
            assert exception.msg == None
        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Failed to seed cluster"

        self.clientreaduserright = aerospike.client(config).connect(
            user, "newpassword")

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_none_username(self):

        policy = {}
        user = None
        password = "newpassword"

        try:
            status = self.client.admin_change_password( user, password, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Username should be a string"

    def test_change_password_with_none_password(self):

        policy = {}
        user = "testchangepassworduser"
        password = None

        try:
            status = self.client.admin_change_password( user, password, policy )

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Password should be a string"

    def test_change_password_with_non_existent_user(self):

        policy = {}
        user = "readwriteuser"
        password = "newpassword"

        try:
            status = self.client.admin_change_password( user, password, policy )

        except InvalidUser as exception:
            assert exception.code == 60
            assert exception.msg == "AEROSPIKE_INVALID_USER"

    def test_change_password_with_too_long_password(self):

        user = "testchangepassworduser"
        config = {"hosts": TestChangePassword.hostlist}
        self.clientreaduser = aerospike.client(config).connect(user,
                                                               "aerospike")

        policy = {'timeout': 100}
        password = "password" * 1000

        status = self.clientreaduser.admin_change_password( user, password, policy )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }

        try:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        except InvalidPassword as exception:
            assert exception.code == 62 
            assert exception.msg == None
        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Failed to seed cluster"

        self.clientreaduserright = aerospike.client(config).connect(user,
                                                                    password)

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()
