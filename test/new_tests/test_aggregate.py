# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e
from aerospike import predicates as p

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def add_stream_udf(client):
    client.udf_put("stream_example.lua", 0)


def remove_stream_udf(client):
    client.udf_remove("stream_example.lua")


def add_required_index(client):
    client.index_integer_create(
        'test', 'demo', 'test_age',
        'age_index')


def remove_index(client):
    client.index_remove('test', 'age_index')


@pytest.mark.xfail(reason="file permissions can cause this to fail")
class TestAggregate(object):

    def setup_class(cls):
        cls.connection_setup_functions = (add_required_index, add_stream_udf)

        cls.connection_teardown_functions = (remove_index, remove_stream_udf)

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        """
        Setup Method
        """

        as_connection = connection_with_config_funcs
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'test_age': i,
                'no': i
            }
            as_connection.put(key, rec)

        def teardown():
            """
            Teardown Method
            """
            for i in range(5):
                key = ('test', 'demo', i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_pos_aggregate_with_where_bool_value(self):
        """
            Invoke aggregate() with where is bool value
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', True, True))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 1

    def test_pos_aggregate_with_where_equals_value(self):
        """
            Invoke aggregate() with where is equal
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 2))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 1

    def test_pos_aggregate_with_empty_module_function(self):
        """
            Invoke aggregate() with empty module and function
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('', '')

        result = None

        def user_callback(value):
            _ = value

        query.foreach(user_callback)
        assert result is None

    def test_pos_aggregate_with_correct_parameters(self):
        """
            Invoke aggregate() with correct arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_pos_aggregate_with_policy(self):
        """
            Invoke aggregate() with policy
        """
        policy = {'timeout': 1000}
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback, policy)
        assert records[0] == 4

    def test_pos_aggregate_with_extra_parameters_to_lua(self):
        """
            Invoke aggregate() with extra arguments
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        stream = None
        query.apply('stream_example', 'count', [stream])

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_pos_aggregate_with_extra_parameter_in_lua(self):
        """
            Invoke aggregate() with extra parameter in lua
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count_extra')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_pos_aggregate_with_arguments_to_lua_function(self):
        """
            Invoke aggregate() with unicode arguments to lua function.
        """
        query = self.as_connection.query('test', 'demo')
        query.where(p.between('test_age', 0, 5))
        query.apply('stream_example', 'group_count', [u"name", u"addr"])

        rec = []

        def callback(value):
            rec.append(value)

        query.foreach(callback)
        assert rec == [
            {u'name4': 1,
             u'name2': 1,
             u'name3': 1,
             u'name0': 1,
             u'name1': 1}
        ]

    def test_pos_aggregate_with_unicode_module_and_function_name(self):
        """
            Invoke aggregate() with unicode module and function names
        """
        query = self.as_connection.query('test', 'demo')
        query.select(u'name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply(u'stream_example', u'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_pos_aggregate_with_multiple_foreach_on_same_query_object(self):
        """
            Invoke aggregate() with multiple foreach on same query object.
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

        records = []
        query.foreach(user_callback)
        assert records[0] == 4

    def test_pos_aggregate_with_multiple_results_call_on_same_query_object(self):
        """
            Invoke aggregate() with multiple foreach on same query object.
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = query.results()
        assert records[0] == 4

        records = []
        records = query.results()
        assert records[0] == 4

    @pytest.mark.skip(reason="This function does not exist")
    def test_neg_aggregate_with_arguments_to_lua_function_having_float_value(self):
        """
            Invoke aggregate() with unicode arguments to lua function having a
            float value
        """
        query = self.as_connection.query('test', 'demo')
        query.where(p.between('test_age', 0, 5))
        query.apply(
            'stream_example', 'double_group_count', [u"name", u"addr", 2.5])

        rec = []

        def callback(value):
            rec.append(value)

        try:
            query.foreach(callback)

        except e.ClientError as exception:
            assert exception.code == -1

    def test_neg_aggregate_with_correct_parameters_without_connection(self):
        """
            Invoke aggregate() with correct arguments without connection
        """
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        try:
            query = client1.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('stream_example', 'count')

            records = []

            def user_callback(value):
                records.append(value)

            query.foreach(user_callback)

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_aggregate_with_extra_parameter(self):
        """
            Invoke aggregate() with extra parameter
        """
        policy = {'timeout': 1000}

        with pytest.raises(TypeError) as typeError:
            query = self.as_connection.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('stream_example', 'count')

            def user_callback(value):
                _ = value

            query.foreach(user_callback, policy, "")

        assert "foreach() takes at most 2 arguments (3 given)" in str(
            typeError.value)

    def test_neg_aggregate_with_no_parameters(self):
        """
            Invoke aggregate() without any mandatory parameters.
        """
        try:
            query = self.as_connection.query()
            query.select()
            query.where()

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_aggregate_no_sec_index(self):
        """
            Invoke aggregate() with no secondary index
        """
        try:
            query = self.as_connection.query('test', 'demo')
            query.select('name', 'no')
            query.where(p.between('no', 1, 5))
            query.apply('stream_example', 'count')

            def user_callback(value):
                _ = value

            query.foreach(user_callback)
        except e.IndexNotFound as exception:
            assert exception.code == 201

    def test_neg_aggregate_with_incorrect_ns_set(self):
        """
            Invoke aggregate() with incorrect ns and set
        """
        try:
            query = self.as_connection.query('test1', 'demo1')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))
            query.apply('stream_example', 'count')

            def user_callback(value):
                _ = value

            query.foreach(user_callback)

        except e.InvalidRequest as exception:
            assert exception.code == 4
        except e.NamespaceNotFound as exception:
            assert exception.code == 20

    def test_neg_aggregate_with_where_incorrect(self):
        """
            Invoke aggregate() with where is incorrect
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 165))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records == []

    def test_neg_aggregate_with_where_none_value(self):
        """
            Invoke aggregate() with where is null value
        """
        query = self.as_connection.query('test', 'demo')
        query.select('name', 'test_age')
        try:
            query.where(p.equals('test_age', None))
            query.apply('stream_example', 'count')

            def user_callback(value):
                _ = value

            query.foreach(user_callback)

        except e.ParamError as exception:
            assert exception.code == -2
