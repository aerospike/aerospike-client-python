# -*- coding: utf-8 -*-

import pytest
import sys
import time

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestChangePassword(object):

    def setup_method(self, method):
        
        """
        Setup method
        """
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        self.client = aerospike.client(config).connect( "admin", "admin" )

        self.client.admin_create_user( {}, "testchangepassworduser", "aerospike", ["read"], 1)
        time.sleep(2)
        self.delete_users = []

    def teardown_method(self, method):

        """
        Teardown method
        """

        self.client.admin_drop_user( {}, "testchangepassworduser" )

        self.client.close()

    def test_change_password_without_any_parameters(self):

    	with pytest.raises(TypeError) as typeError:
    		status = self.client.admin_change_password()

    	assert "Required argument 'policy' (pos 1) not found" in typeError.value

    def test_change_password_with_proper_parameters(self):

    	user = "testchangepassworduser"
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

    	policy = {}
    	password = "newpassword"

    	status = self.clientreaduser.admin_change_password( policy, user, password )

    	assert status == 0

        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

    	assert exception.value[0] == 62
    	assert exception.value[1] == "AEROSPIKE_INVALID_PASSWORD"

        self.clientreaduserright = aerospike.client(config).connect( user, "newpassword" )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()       

    def test_change_password_with_invalid_timeout_policy_value(self):

    	policy = { 'timeout' : 0.1 }
    	user = "testchangepassworduser"
    	password = "newpassword"

    	with pytest.raises(Exception) as exception :
    		status = self.client.admin_change_password( policy, user, password )

    	assert exception.value[0] == -2
    	assert exception.value[1] == "timeout is invalid"

    def test_change_password_with_proper_timeout_policy_value(self):

    	user = "testchangepassworduser"
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

        policy = {'timeout': 10}
        password = "newpassword"

    	status = self.clientreaduser.admin_change_password( policy, user, password )

    	assert status == 0

        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

    	assert exception.value[0] == 62
    	assert exception.value[1] == "AEROSPIKE_INVALID_PASSWORD"

        self.clientreaduserright = aerospike.client(config).connect( user, "newpassword" )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()

    def test_change_password_with_none_username(self):

    	policy = {}
    	user = None
    	password = "newpassword"

    	with pytest.raises(Exception) as exception:
    		status = self.client.admin_change_password( policy, user, password )

    	assert exception.value[0] == -2
    	assert exception.value[1] == "Username should be a string"

    def test_change_password_with_none_password(self):

    	policy = {}
    	user = "testchangepassworduser"
    	password = None

    	with pytest.raises(Exception) as exception:
    		status = self.client.admin_change_password( policy, user, password )

    	assert exception.value[0] == -2
    	assert exception.value[1] == "Password should be a string"

    def test_change_password_with_non_existent_user(self):

    	policy = {}
    	user = "readwriteuser"
    	password = "newpassword"

    	with pytest.raises(Exception) as exception:
    		status = self.client.admin_change_password( policy, user, password )

    	assert exception.value[0] == 60
    	assert exception.value[1] == "aerospike change password failed"

    def test_change_password_with_too_long_password(self):

    	user = "testchangepassworduser"
        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        self.clientreaduser = aerospike.client(config).connect( user, "aerospike" )

        policy = {'timeout': 10}
        password = "password"*1000

    	status = self.clientreaduser.admin_change_password( policy, user, password )

    	assert status == 0

        config = {
                "hosts": [("127.0.0.1", 3000)]
                }
        with pytest.raises(Exception) as exception:
            self.clientreaduserwrong = aerospike.client(config).connect( user, "aerospike" )

    	assert exception.value[0] == 62
    	assert exception.value[1] == "AEROSPIKE_INVALID_PASSWORD"

        self.clientreaduserright = aerospike.client(config).connect( user, password )

        assert self.clientreaduserright != None

        self.clientreaduserright.close()
        self.clientreaduser.close()
