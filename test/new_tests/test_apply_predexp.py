# -*- coding: utf-8 -*-

import pytest
import sys

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import predexp as as_predexp
except:
    print("Please install aerospike python client.")
    sys.exit(1)


'''
Summary of functions:
Sample:
list_append(binname, val): appends an item to the end of a list
list_append_extra(binname, val, extra_arg): same as above, extra arg is unused

TestRecord:
bin_udf_operation_integer(record, bin_name, x, y):
    adds x + y to record[bin_name]
bin_udf_operation_string(record, bin_name, str):
    concatenates str to record[bin_name]
bin_udf_operation_bool(record, bin_name): returns record[bin_name]
list_iterate(record, bin, index_of_ele): equivalent to:
    record[bin].append(58)
    record[bin][index_of_ele] = 222
list_iterate_returns_list(record, bin, index_of_ele):
    Same as previous but returns the modified list

map_iterate(record, bin, set_value): Changes every value
    in the map map[bin] to be equal to set_value

map_iterate_returns_map(record, bin, set_value):
    Same as above but returns the map
function udf_returns_record(rec):
    Return a map representation of a record
udf_without_arg_and_return(record):
    does nothing and returns nothing
function udf_put_bytes(record, bin):
    creates an array of bytes and inserts it in record[bin]

BasicOps:

create_record(rec, value)
    creates a new record with the data
        record['bin'] = value
'''


def add_indexes_and_udfs(client):
    '''
    Load the UDFs used in the tests and setup indexes
    '''
    policy = {}
    try:
        client.index_integer_create(
            'test', 'demo', 'age', 'age_index', policy)
    except e.IndexFoundError:
        pass
    try:
        client.index_integer_create(
            'test', 'demo', 'age1', 'age_index1', policy)
    except e.IndexFoundError:
        pass

    udf_type = 0
    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_put(module, udf_type, policy)


def remove_indexes_and_udfs(client):
    '''
    Remove all of the UDFS and indexes created for these tests
    '''
    policy = {}

    try:
        client.index_remove('test', 'age_index', policy)
    except e.IndexNotFound:
        pass

    try:
        client.index_remove('test', 'age_index1', policy)
    except e.IndexNotFound:
        pass

    udf_files = ("sample.lua", "test_record_udf.lua", "udf_basic_ops.lua")

    for module in udf_files:
        client.udf_remove(module, policy)


class TestApply(TestBaseClass):

    def setup_class(cls):
        # Register setup and teardown functions
        cls.connection_setup_functions = [add_indexes_and_udfs]
        cls.connection_teardown_functions = [remove_indexes_and_udfs]

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {
                'name': ['name%s' % (str(i))],
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i,
                'basic_map': {"k30": 6,
                              "k20": 5,
                              "k10": 1}
            }
            as_connection.put(key, rec)

        def teardown():
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "func_args, test_bin, predexp, expected",
        (
            (
                ['name', 1],
                'name',
                [
                as_predexp.integer_bin('age'),
                as_predexp.integer_value(1),
                as_predexp.integer_equal()
                ],
                ['name1', 1]
            ),
        ), ids=[
            "Integer",
        ]
    )
    def test_apply_causing_list_append_with_correct_params_with_predexp(
            self, func_args, test_bin, predexp, expected):
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(
            key, 'sample', 'list_append', func_args, policy={'predexp': predexp})

        _, _, bins = self.as_connection.get(key)

        assert bins[test_bin] == expected
        assert retval == 0  # the list_append UDF returns 0
