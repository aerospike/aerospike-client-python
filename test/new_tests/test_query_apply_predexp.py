# -*- coding: utf-8 -*-
import pytest
import time
import sys
import pickle
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p
from aerospike import predexp as as_predexp

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def add_indexes_to_client(client):
    try:
        client.index_integer_create('test', 'demo', 'age',
                                    'test_demo_age_idx')
    except e.IndexFoundError:
        pass

    try:
        client.index_integer_create('test', None, 'age',
                                    'test_null_age_idx')
    except e.IndexFoundError:
        pass


def create_records(client):
    for i in range(1, 10):
        key = ('test', 'demo', i)
        rec = {'name': str(i), 'age': i, 'val': i}
        client.put(key, rec)

    key = ('test', None, "no_set")
    rec = {'name': 'no_set_name', 'age': 0}
    client.put(key, rec)


def drop_records(client):
    for i in range(1, 10):
        key = ('test', 'demo', i)
        try:
            client.remove(key)
        except e.RecordNotFound:
            pass

    try:
        client.remove(('test', None, "no_set"))
    except e.RecordNotFound:
        pass


def add_test_udf(client):
    policy = {}
    client.udf_put(u"query_apply.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove(u"query_apply.lua")


def add_test_parameter_udf(client):
    policy = {}
    client.udf_put(u"query_apply_parameters.lua", 0, policy)


def drop_test_parameter_udf(client):
    client.udf_remove(u"query_apply_parameters.lua")


def remove_indexes_from_client(client):
    client.index_remove('test', 'test_demo_age_idx')
    client.index_remove('test', 'test_null_age_idx')


class TestQueryApply(object):

    # These functions will run once for this test class, and do all of the
    # required setup and teardown
    connection_setup_functions = (add_test_udf, add_test_parameter_udf,
     add_indexes_to_client, create_records)
    connection_teardown_functions = (drop_test_udf, drop_test_parameter_udf,
     remove_indexes_from_client, drop_records)
    age_range_pred = p.between('age', 0, 4)  # Predicate for ages between [0,5)
    no_set_key = ('test', None, "no_set")  # Key for item stored in a namespace but not in a set

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        client = connection_with_config_funcs
        create_records(client)

    def test_query_apply_with_new_predexp(self):
        """
        Invoke query_apply() with correct policy and predexp
        """

        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        predexp = [
            as_predexp.integer_bin('age'),
            as_predexp.integer_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('val'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        policy = {'total_timeout': 0, 'predexp': predexp}
        query_id = self.as_connection.query_apply(
            "test", "demo", self.age_range_pred, "query_apply",
            "mark_as_applied", ['name', 2], policy)

        self._wait_for_query_complete(query_id)

        recs = []

        for i in range(1, 10):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'aerospike':
                recs.append(bins)
        
        assert len(recs) == 2
        for rec in recs:
            assert rec['age'] == 2 or rec['val'] == 3

    def test_query_apply_with_bad_new_predexp(self):
        """
        Invoke query_apply() with correct policy and predexp
        """

        predexp = [
            as_predexp.integer_bin('age'),
            as_predexp.string_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('val'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        policy = {'total_timeout': 0, 'predexp': predexp}
        with pytest.raises(e.ParamError):
            query_id = self.as_connection.query_apply(
                "test", "demo", self.age_range_pred, "query_apply",
                "mark_as_applied", ['name', 2], policy)

    def _wait_for_query_complete(self, query_id):
        while True:
            response = self.as_connection.job_info(
                query_id, aerospike.JOB_QUERY)
            if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
                return
            time.sleep(0.1)