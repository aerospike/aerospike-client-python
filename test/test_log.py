# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)
        
def handler(level, func, myfile, line):
    assert 1 == 1

def extrahandler(level, func, myfile, line):
    print "Level is: %d" % level

class TestLog(object):
    def test_set_log_level_correct(self):
        """
        Test log level with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)

        assert response == 0

    def test_set_log_handler_correct(self):
        """
        Test log handler with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(handler)

        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                "hosts": hostlist
                }
        if user == None and password == None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)

        assert response == 0
        client.close()

    def test_set_log_level_None(self):
        """
        Test log level with log level as None
        """
        try:
            response = aerospike.set_log_level(None)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Invalid log level'

    def test_set_log_level_incorrect(self):
        """
        Test log level with log level incorrect
        """
        response = aerospike.set_log_level(9)

        assert response == 0

    def test_set_log_handler_extra_parameter(self):
        """
        Test log handler with extra parameter
        """

        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)

        with pytest.raises(TypeError) as typeError:
            aerospike.set_log_handler(handler, extrahandler)

        assert "setLogHandler() takes at most 1 argument (2 given)" in typeError.value
