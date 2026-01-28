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

    def test_set_log_level_with_correct_argument(self):
        response = aerospike.set_log_level(loglevel=aerospike.LOG_LEVEL_DEBUG)
        assert response == 0

    def test_set_log_handler_with_no_args(self):
        """
        Test default log handler
        """
        response = aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)
        assert response == 0

        aerospike.set_log_handler()

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()
        client.close()

        # We don't test for live stdout (via capsys) because Python can "block buffer" stdout by default
        # instead of line buffering. This test will fail if "block buffering" is enabled since
        # the logs will not be printed out until the end when the tests have finished.

    # Also test all the log levels
    @pytest.mark.parametrize(
        "log_level, expected_log_line_count",
        [
            (aerospike.LOG_LEVEL_TRACE, 1),
            (aerospike.LOG_LEVEL_DEBUG, 0),
            (aerospike.LOG_LEVEL_INFO, 0),
            (aerospike.LOG_LEVEL_WARN, 0),
            (aerospike.LOG_LEVEL_ERROR, 0),
            (aerospike.LOG_LEVEL_OFF, 0)
        ]
    )
    def test_set_log_handler_with_correct_callback_argument(self, log_level, expected_log_line_count):
        log_tuples = []
        def custom_log_callback(level, func, path, line, msg):
            assert type(level) == int
            assert type(func) == str
            assert type(path) == str
            assert type(line) == int
            assert type(msg) == str

            log_tuple = (level, func, path, line, msg)
            log_tuples.append(log_tuple)

        aerospike.set_log_level(log_level)
        aerospike.set_log_handler(log_handler=custom_log_callback)

        # Forces a single event to be logged
        add_config = {
            "validate_keys": True,
            "invalid_option": True
        }
        with pytest.raises(e.ParamError):
            # Only one log line at most should be produced from the Python client's glue code
            TestBaseClass.get_new_connection(add_config)

        assert len(log_tuples) == expected_log_line_count

    def test_set_log_handler_correct_with_none_argument(self):
        """
        Test that log handler was removed
        """
        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(None)

        # Forces an event to be logged
        client = TestBaseClass.get_new_connection()
        client.close()

        # See comment in test_set_log_handler_with_no_args why we don't use capsys to check stdout

    @pytest.mark.parametrize(
        "log_level",
        [
            # Larger than max value of long int
            68786586756785785745,
            None, [], {}, 1.5, "serious"
        ]
    )
    def test_set_log_handler_with_invalid_log_level(self, log_level):
        with pytest.raises(e.ParamError):
            aerospike.set_log_level(log_level)

    # TODO: undefined behavior
    def test_set_log_level_incorrect(self):
        """
        Test log level with a log level of valid type, but outside of
        expected range
        """
        response = aerospike.set_log_level(9)
        assert response == 0

    def test_set_log_level_invalid_arg_count(self):
        with pytest.raises(TypeError):
            aerospike.set_log_level()

    def test_set_log_handler_invalid_arg_count(self):
        with pytest.raises(TypeError):
            aerospike.set_log_handler(None, None)
