# -*- coding: utf-8 -*-

import pytest
import sys

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike_helpers import expressions as exp
    from aerospike import exception as e
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
        "func_args, test_bin, expected",
        (
            (['name', 'car'], 'name', ['name1', 'car']),
            (['name', None], 'name', ['name1', None]),
            (['name', 1], 'name', ['name1', 1]),
            (['name', [1, 2]], 'name', ['name1', [1, 2]]),
            (['name', {'a': 'b'}], 'name', ['name1', {'a': 'b'}]),
        ), ids=[
            "string",
            "None",
            "Integer",
            "list",
            "dict"
        ]

    )
    def test_apply_causing_list_append_with_correct_params(
            self, func_args, test_bin, expected):

        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(
            key, 'sample', 'list_append', func_args)

        _, _, bins = self.as_connection.get(key)

        assert bins[test_bin] == expected
        assert retval == 0  # the list_append UDF returns 0

    @pytest.mark.parametrize(
        "func_args, test_bin, expressions, expected",
        (
            (
                ['name', 1],
                'name',
                exp.Eq(exp.IntBin('age'), 1),
                ['name1', 1]
            ),
        ), ids=[
            "Integer",
        ]
    )
    def test_apply_causing_list_append_with_correct_params_with_expressions(
            self, func_args, test_bin, expressions, expected):

        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(
            key, 'sample', 'list_append', func_args, policy={'expressions': expressions.compile()})

        _, _, bins = self.as_connection.get(key)

        assert bins[test_bin] == expected
        assert retval == 0  # the list_append UDF returns 0

    def test_apply_return_bool_true(self):
        """
            Invoke apply() with UDF that will return booleans.
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'bool_check', [])
        assert retval == True

    def test_apply_return_bool_false(self):
        """
            Invoke apply() with UDF that will return booleans.
        """
        key = ('test', 'demo', 'non_existent_record')
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'bool_check', [])
        assert retval == False

    def test_apply_operations_on_map(self):
        """
            Invoke apply() with map
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'map_iterate', ['basic_map', 555])
        assert retval is None
        # Since this udf changes all of the record values to be 555,
        # all of the values in the 'basic_map' should be 555
        (key, _, bins) = self.as_connection.get(key)
        assert bins['basic_map'] == {"k30": 555, "k20": 555, "k10": 555}

    def test_apply_with_correct_parameters_float_argument(self):
        """
            Invoke apply() with correct arguments with a floating value in the
            list of arguments
        """
        if self.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on float type as lua argument")
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(
            key, 'sample', 'list_append', ['name', 5.434])
        _, _, bins = self.as_connection.get(key)

        assert bins['name'] == ['name1', 5.434]
        assert retval == 0

    def test_apply_with_policy(self):
        """
            Invoke apply() with policy
        """
        policy = {'total_timeout': 1000}
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(
            key, 'sample', 'list_append', ['name', 'car'], policy)
        (key, _, bins) = self.as_connection.get(key)
        assert retval == 0
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_integer(self):
        """
            Invoke apply() with integer
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'bin_udf_operation_integer',
                                          ['age', 2, 20])

        assert retval == 23
        (key, _, bins) = self.as_connection.get(key)
        assert bins['age'] == 23

    def test_apply_with_operation_on_string(self):
        """
            Invoke apply() with string
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'bin_udf_operation_string',
                                          ['addr', " world"])

        assert retval == "name1 world"
        (key, _, bins) = self.as_connection.get(key)
        assert bins['addr'] == "name1 world"

    def test_apply_with_function_returning_record(self):
        """
            Invoke apply() with record
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'test_record_udf',
                                          'udf_returns_record', [])

        # This function returns a record which gets mapped to a python dict
        assert retval is not None
        assert isinstance(retval, dict)

    def test_apply_using_bytearray(self):
        """
            Invoke apply() with a bytearray as a argument
        """
        key = ('test', 'demo', 'apply_insert')
        self.as_connection.apply(key, 'udf_basic_ops',
                                 'create_record',
                                 [bytearray("asd;as[d'as;d",
                                            "utf-8")])
        (key, _, bins) = self.as_connection.get(key)

        assert bins == {'bin': bytearray(b"asd;as[d\'as;d")}

        self.as_connection.remove(key)

    def test_apply_with_extra_argument_to_lua_function(self):
        """
            Invoke apply() with extra argument to lua
            Should not raise an error
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'sample', 'list_append',
                                          ['name', 'car', 1])
        assert retval == 0
        (key, _, bins) = self.as_connection.get(key)
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_missing_argument_to_lua(self):
        """
            Invoke apply() passing less than the ex arguments
            to a lua function. Should not raise an error
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, 'sample', 'list_append_extra',
                                          ['name', 'car'])
        assert retval == 0
        (key, _, bins) = self.as_connection.get(key)
        assert bins['name'] == ['name1', 'car']

    def test_apply_with_unicode_module_and_function(self):
        """
            Invoke apply() with unicode module and function
        """
        key = ('test', 'demo', 1)
        retval = self.as_connection.apply(key, u'sample', u'list_append',
                                          ['name', 'car'])
        (key, _, bins) = self.as_connection.get(key)

        assert bins['name'] == ['name1', 'car']
        assert retval == 0

    def test_apply_with_correct_parameters_without_connection(self):
        """
            Invoke apply() with correct arguments without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.apply(key, 'sample', 'list_append', ['name', 'car'])

        err_code = err_info.value.code

        assert err_code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_apply_with_arg_causing_error(self):
        """
            Invoke apply() with ia string argument when integer is required
        """
        key = ('test', 'demo', 1)

        with pytest.raises(e.UDFError) as err_info:
            self.as_connection.apply(key, 'test_record_udf',
                                     'bin_udf_operation_integer',
                                     ['age', "not an", "integer"])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_UDF

    def test_apply_with_no_parameters(self):
        """
            Invoke apply() without any mandatory parameters.
            This should raise an error
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.apply()

        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_apply_with_no_argument_in_lua(self):
        """
            Call apply without the args parameter
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.as_connection.apply(key, 'sample', 'list_append_extra')
        assert "argument 'args' (pos 4)" in str(
            typeError.value)

    def test_apply_with_incorrect_policy(self):
        """
            Invoke apply() with incorrect policy
        """
        policy = {'total_timeout': 0.1}
        key = ('test', 'demo', 1)
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.apply(key, 'sample', 'list_append',
                                     ['name', 'car'], policy)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_apply_with_extra_argument(self):
        """
            Invoke apply() with extra argument.
        """
        policy = {'total_timeout': 1000}
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.as_connection.apply(key, 'sample', 'list_append',
                                     ['name', 'car'], policy, "")

        assert "apply() takes at most 5 arguments (6 given)" in str(
            typeError.value)

    @pytest.mark.parametrize(
        "module, function, arguments",
        (
            ('sample', 'list_append', ['addr', 'car']),
            ('', '', ['name', 'car']),
            ('wrong_module_name', 'list_append', ['name', 'car']),
            ('sample', 'list_prepend', ['name', 'car'])
        ),
        ids=[
            'incorrect argument type',
            'empty module and function',
            'non existent module',
            'non existent function'
        ]
    )
    def test_udf_error_causing_args(self, module, function, arguments):
        key = ('test', 'demo', 1)

        with pytest.raises(e.UDFError) as err_info:
            self.as_connection.apply(key, module, function, arguments)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_UDF

    def test_apply_with_key_as_string(self):
        """
            Invoke apply() with key as string
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.apply(
                "", 'sample', 'list_append', ['name', 'car'])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_apply_with_key_as_none(self):
        """
            Invoke apply() with key as none
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.apply(
                None, 'sample', 'list_append', ['name', 'car'])

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_apply_with_incorrect_ns_set(self):
        """
            Invoke apply() with incorrect ns and set
        """

        with pytest.raises(e.ClientError) as err_info:
            key = ('test1', 'demo', 1)
            self.as_connection.apply(key, 'sample', 'list_prepend',
                                     ['name', 'car'])
