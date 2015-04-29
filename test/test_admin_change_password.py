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

class TestChangePassword(TestBaseClass):

    def setup_method(self, method):
        
        """
        Setup method
        """
        hostlist, user, password = TestBaseClass().get_hosts()
        config = {
                "hosts": hostlist
                }
        self.client = aerospike.client(config).connect( user, password )

        self.client.admin_create_user( "testchangepassworduser", "aerospike", ["read"], {})
        time.sleep(2)
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
        config = {
                "hosts": TestChangePassword.hostlist
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

        policy = {}
        password = "newpassword"

        status = self.clientreaduser.admin_change_password( user, password )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        status = [-1L, 62]
        for val in status:
            if exception.value[0] != val:
                continue
            else:
                break

        assert exception.value[0] == val

        self.clientreaduserright = aerospike.client(config).connect( user, "newpassword" )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_invalid_timeout_policy_value(self):

        policy = { 'timeout' : 0.1 }
        user = "testchangepassworduser"
        password = "newpassword"

        with pytest.raises(Exception) as exception :
            status = self.client.admin_change_password( user, password, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_change_password_with_proper_timeout_policy_value(self):

        user = "testchangepassworduser"
        config = {
                "hosts": TestChangePassword.hostlist
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

        policy = {'timeout': 10}
        password = "newpassword"

        status = self.clientreaduser.admin_change_password( user, password, policy )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        status = [-1L, 62]
        for val in status:
            if exception.value[0] != val:
                continue
            else:
                break

        assert exception.value[0] == val

        self.clientreaduserright = aerospike.client(config).connect( user, "newpassword" )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_none_username(self):

        policy = {}
        user = None
        password = "newpassword"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_change_password( user, password, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "Username should be a string"

    def test_change_password_with_none_password(self):

        policy = {}
        user = "testchangepassworduser"
        password = None

        with pytest.raises(Exception) as exception:
            status = self.client.admin_change_password( user, password, policy )

        assert exception.value[0] == -2
        assert exception.value[1] == "Password should be a string"

    def test_change_password_with_non_existent_user(self):

        policy = {}
        user = "readwriteuser"
        password = "newpassword"

        with pytest.raises(Exception) as exception:
            status = self.client.admin_change_password( user, password, policy )

        assert exception.value[0] == 60
        assert exception.value[1] == "AEROSPIKE_INVALID_USER"

    def test_change_password_with_too_long_password(self):

        user = "testchangepassworduser"
        config = {
                "hosts": TestChangePassword.hostlist
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

        policy = {'timeout': 10}
        password = "password"*1000

        status = self.clientreaduser.admin_change_password( user, password, policy )

        assert status == 0

        config = {
                "hosts": TestChangePassword.hostlist
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

        status = [-1L, 62]
        for val in status:
            if exception.value[0] != val:
                continue
            else:
                break

        assert exception.value[0] == val

        self.clientreaduserright = aerospike.client(config).connect( user, password )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()
