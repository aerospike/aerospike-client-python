# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass
import os
import shutil
import time

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

from aerospike import predicates as p

class TestAggregate(TestBaseClass):
    def setup_class(cls):
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
            'hosts': hostlist,
            'lua':{'user_path': '/tmp/',
            'system_path':'../aerospike-client-c/lua/'}}
        if user == None and password == None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)

        TestAggregate.skip_old_server = True
        versioninfo = client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value != None:
                    versionlist = value[value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                        TestAggregate.skip_old_server = False
        client.index_integer_create('test', 'demo', 'test_age',
                'test_demo_test_age_idx')
        client.index_integer_create('test', 'demo', 'age1', 'test_demo_age1_idx')
        time.sleep(2)

        filename = "stream_example.lua"
        udf_type = aerospike.UDF_TYPE_LUA
        status = client.udf_put(filename, udf_type)
        shutil.copyfile(filename, config['lua']['user_path'] +
            'stream_example.lua')
        client.close()

    def teardown_class(cls):
        return
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
            'hosts': hostlist,
            'lua':{'user_path': '/tmp/',
            'system_path':'../aerospike-client-c/lua/'}}
        if user == None and password == None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)
        client.index_remove('test', 'test_demo_test_age_idx')
        client.index_remove('test', 'test_demo_age1_idx')
        module = "stream_example.lua"

        status = client.udf_remove(module)
        os.remove(config['lua']['user_path'] + 'stream_example.lua')
        client.close()

    def setup_method(self, method):
        """
        Setup method.
        """

        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
            'hosts': hostlist,
            'lua':{'user_path': '/tmp/',
            'system_path':'../aerospike-client-c/lua/'}}
        if TestBaseClass.user == None and TestBaseClass.password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(
                TestBaseClass.user, TestBaseClass.password)

        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'test_age': i,
                'no': i
            }
            self.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            #self.client.remove(key)
        self.client.close()

    def test_aggregate_with_no_parameters(self):
        """
            Invoke aggregate() without any mandatory parameters.
        """
        try:
            query = self.client.query()
            query.select()
            query.where()

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'query() expects atleast 1 parameter'

        #assert "where() takes at least 1 argument (0 given)" in typeError.value

    def test_aggregate_no_sec_index(self):
        """
            Invoke aggregate() with no secondary index
        """
        try:
            query = self.client.query('test', 'demo')
            query.select('name', 'no')
            query.where(p.between('no', 1, 5))
            query.apply('stream_example', 'count')

            result = None

            def user_callback(value):
                result = value

            query.foreach(user_callback)
        except IndexNotFound as exception:
            assert exception.code == 201L
            assert exception.msg == 'AEROSPIKE_ERR_INDEX_NOT_FOUND'

    def test_aggregate_with_incorrect_ns_set(self):
        """
            Invoke aggregate() with incorrect ns and set
        """
        try:
            query = self.client.query('test1', 'demo1')
            query.select('name', 'test_age')
            query.where(p.equals('test_age', 1))
            query.apply('stream_example', 'count')
            result = 1

            def user_callback(value):
                result = value

            query.foreach(user_callback)

        except InvalidRequest as exception:
            assert exception.code == 4L
            assert exception.msg == 'AEROSPIKE_ERR_REQUEST_INVALID'

    #@pytest.mark.xfail(reason="C client incorrectly sent status AEROSPIKE_ERR_UDF")
    def test_aggregate_with_where_incorrect(self):
        """
            Invoke aggregate() with where is incorrect
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 165))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records == []

    def test_aggregate_with_where_none_value(self):
        """
            Invoke aggregate() with where is null value
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        try:
            query.where(p.equals('test_age', None))
            query.apply('stream_example', 'count')
            result = 1

            def user_callback(value):
                result = value

            query.foreach(user_callback)

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == 'predicate is invalid.'

    #@pytest.mark.xfail(reason="C client incorrectly sent status AEROSPIKE_ERR_UDF")
    def test_aggregate_with_where_bool_value(self):
        """
            Invoke aggregate() with where is bool value
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', True, True))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 1

    def test_aggregate_with_where_equals_value(self):
        """
            Invoke aggregate() with where is equal
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.equals('test_age', 2))
        query.apply('stream_example', 'count')
        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 1

    def test_aggregate_with_empty_module_function(self):
        """
            Invoke aggregate() with empty module and function
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('', '')

        result = None

        def user_callback(value):
            result = value

        query.foreach(user_callback)
        assert result == None
    """
    def test_aggregate_with_incorrect_module(self):
            #Invoke aggregate() with incorrect module
        try:
            query = self.client.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('streamwrong', 'count')

            result = None

            def user_callback(value):
                result = value

            query.foreach(user_callback)

        except ClientError as exception:
            assert exception.code == -1L
            assert exception.msg == 'UDF: Execution Error 1'

    def test_aggregate_with_incorrect_function(self):
        #Invoke aggregate() with incorrect function
        try:
            query = self.client.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('stream_example', 'countno')

            records = []

            def user_callback(value):
                records.append(value)

            query.foreach(user_callback)
        except ClientError as exception:
            assert exception.code == -1L
            assert exception.msg == 'UDF: Execution Error 2 : function not found'
            """
    def test_aggregate_with_correct_parameters(self):
        """
            Invoke aggregate() with correct arguments
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_aggregate_with_policy(self):
        """
            Invoke aggregate() with policy
        """
        policy = {'timeout': 1000}
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback, policy)
        assert records[0] == 4

    def test_aggregate_with_extra_parameter(self):
        """
            Invoke aggregate() with extra parameter
        """
        policy = {'timeout': 1000}

        with pytest.raises(TypeError) as typeError:
            query = self.client.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('stream_example', 'count')

            result = None

            def user_callback(value):
                result = value

            query.foreach(user_callback, policy, "")

        assert "foreach() takes at most 2 arguments (3 given)" in typeError.value

    def test_aggregate_with_extra_parameters_to_lua(self):
        """
            Invoke aggregate() with extra arguments
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        stream = None
        query.apply('stream_example', 'count', [stream])

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_aggregate_with_extra_parameter_in_lua(self):
        """
            Invoke aggregate() with extra parameter in lua
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count_extra')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    """
    def test_aggregate_with_less_parameter_in_lua(self):
        #Invoke aggregate() with less parameter in lua
        try:
            query = self.client.query('test', 'demo')
            query.select('name', 'test_age')
            query.where(p.between('test_age', 1, 5))
            query.apply('stream_example', 'count_less')

            records = []

            def user_callback(value):
                records.append(value)

            query.foreach(user_callback)

        except ClientError as exception:
            assert exception.code == -1L
            """
    def test_aggregate_with_arguments_to_lua_function(self):
        """
            Invoke aggregate() with unicode arguments to lua function.
        """
        query = self.client.query('test', 'demo')
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

    def test_aggregate_with_arguments_to_lua_function_having_float_value(self):
        """
            Invoke aggregate() with unicode arguments to lua function having a
            float value
        """
        if TestAggregate.skip_old_server == True:
            pytest.skip("Server does not support aggregate on float type as lua argument")
        query = self.client.query('test', 'demo')
        query.where(p.between('test_age', 0, 5))
        query.apply('stream_example', 'double_group_count', [u"name", u"addr", 2.5])

        rec = []

        def callback(value):
            rec.append(value)

        query.foreach(callback)
        assert rec == [
            {u'name4': 3.5,
             u'name2': 3.5,
             u'name3': 3.5,
             u'name0': 3.5,
             u'name1': 3.5}
        ]

    def test_aggregate_with_unicode_module_and_function_name(self):
        """
            Invoke aggregate() with unicode module and function names
        """
        query = self.client.query('test', 'demo')
        query.select(u'name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply(u'stream_example', u'count')

        records = []

        def user_callback(value):
            records.append(value)

        query.foreach(user_callback)
        assert records[0] == 4

    def test_aggregate_with_multiple_foreach_on_same_query_object(self):
        """
            Invoke aggregate() with multiple foreach on same query object.
        """
        query = self.client.query('test', 'demo')
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

    def test_aggregate_with_multiple_results_call_on_same_query_object(self):
        """
            Invoke aggregate() with multiple foreach on same query object.
        """
        query = self.client.query('test', 'demo')
        query.select('name', 'test_age')
        query.where(p.between('test_age', 1, 5))
        query.apply('stream_example', 'count')

        records = []
        records = query.results()
        assert records[0] == 4

        records = []
        records = query.results()
        assert records[0] == 4

    def test_aggregate_with_correct_parameters_without_connection(self):
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

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
