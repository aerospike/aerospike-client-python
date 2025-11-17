# -*- coding: utf-8 -*-
import pytest

from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike


class TestLog(object):
    def teardown_class(cls):
        """
        Set the class level logger to a no-op to ensure no problems later
        """
        aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)
        aerospike.set_log_handler(None)

    def test_set_log_level_correct(self):
        """
        Test log level with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)

        assert response == 0

    def test_enable_log_handler_correct_no_callback(self):
        """
        Test log handler with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler()

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()

        assert response == 0
        client.close()

    def test_enable_log_handler_correct_with_callback(self):
        """
        Test log handler with correct parameters
        """
        def log_callback(level, func, path, line, msg):
            print("[{}] {}".format(func, msg))
        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(log_callback)

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()

        assert response == 0
        client.close()

    def test_enable_log_handler_correct_with_none(self):
        """
        Test log handler with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(None)

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()

        assert response == 0
        client.close()

    def test_enable_log_handler_correct_with_LOG_LEVEL_OFF(self):
        """
        Test log handler with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_OFF)
        aerospike.set_log_handler()

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()

        assert response == 0
        client.close()

    def test_enable_log_handler_correct_with_LOG_LEVEL_DEBUG(self):
        """
        Test log handler with correct parameters
        """

        response = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(None)

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()

        assert response == 0
        client.close()

    def test_enable_log_handler_incorrect_with_LOG_LEVEL_value(self):
        """
        Test log handler with correct parameters
        """
        with pytest.raises(e.ParamError) as param_error:
            response = aerospike.set_log_level(68786586756785785745)

        assert param_error.value.code == -2
        assert param_error.value.msg == 'integer value exceeds sys.maxsize'

    @pytest.mark.parametrize("level", [None, [], {}, 1.5, "serious"])
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
