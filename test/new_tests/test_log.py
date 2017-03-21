# -*- coding: utf-8 -*-
import pytest
import sys

from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def handler(level, func, path, line, msg):
    assert 1 == 1


def valid_handler(level, func, path, line, msg):
    pass


def error_raising_handler(level, func, path, line, msg):
    raise Exception("Handler raised an error")


def wrong_args_handler():
    pass


def extrahandler(level, func, myfile, line):
    print ("Level is: %d" % level)


class TestLog(object):

    def teardown_class(cls):
        '''
        Set the class level logger to a no-op to ensure no problems later
        '''
        aerospike.set_log_handler(valid_handler)

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
        if user is None and password is None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)

        assert response == 0
        client.close()

    @pytest.mark.skip()
    def test_incorrect_prototype_callback(self):
        """
        Test that having a callback which takes the wrong number
        of args, will raise an error on methods which trigger
        logging
        """
        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(wrong_args_handler)

        with pytest.raises(SystemError):
            hostlist, user, password = TestBaseClass.get_hosts()
            config = {
                "hosts": hostlist
            }
            if user is None and password is None:
                client = aerospike.client(config).connect()
            else:
                client = aerospike.client(config).connect(user, password)

        try:
            client.close()  # Close the client if it got opened
        except:
            pass

    @pytest.mark.skip()
    def test_log_handler_raising_error(self):
        '''
        Test for handling of a log handler which raises an error
        '''
        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(error_raising_handler)
        with pytest.raises(SystemError):
            hostlist, user, password = TestBaseClass.get_hosts()
            config = {
                "hosts": hostlist
            }
            if user is None and password is None:
                client = aerospike.client(config).connect()
            else:
                client = aerospike.client(config).connect(user, password)

        try:
            client.close()  # Close the client if it got opened
        except:
            pass

    @pytest.mark.xfail(reason="These don't raise errors")
    @pytest.mark.parametrize("callback",
                             [1, 'a', False, None])
    def test_set_log_handler_with_non_callables(self, callback):
        '''
        Test whether a non callable may be set as the log function
        '''
        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        with pytest.raises(e.ParamError):
            aerospike.set_log_handler(callback)

    @pytest.mark.parametrize("level",
                             [None, [], {}, 1.5, 'serious'])
    def test_set_log_level_with_invalid_type(self, level):
        """
        Test set_log_level with non int subtypes
        """
        with pytest.raises(e.ParamError) as param_error:
            aerospike.set_log_level(level)

        assert param_error.value.code == -2

    @pytest.mark.skip(reason="This behavior may or may not be correct")
    def test_set_log_level_with_bool(self):
        """
        Test log level with log level as a bool,
        this works because bool is a subclass of int
        """
        with pytest.raises(e.ParamError) as param_error:
            aerospike.set_log_level(False)

        assert param_error.value.code == -2

    def test_set_log_level_incorrect(self):
        """
        Test log level with a log level of valid type, but outside of
        expected range
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

        assert "setLogHandler() takes at most 1 argument (2 given)" in str(
            typeError.value)
