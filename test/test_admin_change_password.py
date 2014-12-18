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

    	policy = {}
    	user = "testchangepassworduser"
    	password = "newpassword"

    	status = self.client.admin_change_password( policy, user, password )

    	assert status == 0

    def test_change_password_with_invalid_timeout_policy_value(self):

    	policy = { 'timeout' : 0.1 }
    	user = "testchangepassworduser"
    	password = "newpassword"

    	with pytest.raises(Exception) as exception :
    		status = self.client.admin_change_password( policy, user, password )

    	assert exception.value[0] == -2
    	assert exception.value[1] == "Invalid value(type) for policy key"

    def test_change_password_with_proper_timeout_policy_value(self):

    	policy = { 'timeout' : 3 }
    	user = "testchangepassworduser"
    	password = "newpassword"
    	status = self.client.admin_change_password( policy, user, password ) 

    	assert status == 0

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

    	policy = {}
    	user = "testchangepassworduser"
    	password = "password"*1000
    	status = self.client.admin_change_password( policy, user, password )

    	assert status == 0
