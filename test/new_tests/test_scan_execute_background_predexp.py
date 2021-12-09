# -*- coding: utf-8 -*-
import pytest
import time

import aerospike
from aerospike import exception, predicates
from aerospike_helpers.operations import operations, map_operations
from aerospike import predexp as as_predexp

TEST_NS = 'test'
TEST_SET = 'background_scan1'
TEST_SET2 = 'background_scan2'
TEST_SET3 = 'background_scan3'
TEST_UDF_MODULE = 'bin_lua'
TEST_UDF_FUNCTION = 'mytransformscan'
# Hack to get long to exist in Python 3
try:
    long
except NameError:
    long = int

def wait_for_job_completion(as_connection, job_id):
    '''
    Blocks until the job has completed
    '''
    time.sleep(0.1)
    while True:
        response = as_connection.job_info(job_id, aerospike.JOB_SCAN)
        if response['status'] != aerospike.JOB_STATUS_INPROGRESS:
            break
        time.sleep(0.1)


def add_test_udf(client):
    policy = {}
    client.udf_put(u"bin_lua.lua", 0, policy)


def drop_test_udf(client):
    client.udf_remove("bin_lua.lua")


def validate_records(client, keys, validator):
    for key in keys:
        _, _, rec = client.get(key)
        assert validator(rec)


class TestScanApply(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        add_test_udf(self.as_connection)
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]
        for i, key in enumerate(keys):
            self.as_connection.put(key, {'number': i})
        keys = [(TEST_NS, TEST_SET2, i) for i in range(10)]
        for i, key in enumerate(keys):
            self.as_connection.put(key, {'number': i})
        keys = [(TEST_NS, TEST_SET3, i) for i in range(10)]
        for i, key in enumerate(keys):
            self.as_connection.put(key, {'numbers': {1: i, 2: 2 * i, 3: 3 * i}})
        
        
        def teardown():
            drop_test_udf(self.as_connection)
            keys = [(TEST_NS, TEST_SET, i) for i in range(50)]
            for i, key in enumerate(keys):
                self.as_connection.remove(key, {'number': i})
            keys = [(TEST_NS, TEST_SET2, i) for i in range(10)]
            for i, key in enumerate(keys):
                self.as_connection.remove(key, {'number': i})
    
        request.addfinalizer(teardown)

    def test_background_execute_predexp_everywhere(self):
        """
        Ensure that Scan.execute_background() gets applied to records that match the predexp.
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        test_bin = 'number'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]

        predexp = [
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        policy = {
            'predexp': predexp
        }

        scan = self.as_connection.scan(TEST_NS, TEST_SET)
        scan.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin, 1])
        job_id = scan.execute_background(policy)
        wait_for_job_completion(self.as_connection, job_id)

        for i, key in enumerate(keys):
            _, _, bins = self.as_connection.get(key)
            if i == 2 or i == 3:
                assert(bins[test_bin] == i + 1)
            else:
                assert(bins.get(test_bin) == i)

    @pytest.mark.xfail(reason="scan does not implement .where()")
    def test_background_execute_predexp_and_predicate(self):
        """
        Ensure that Scan.execute_background() gets applied to records that match the predicate.
        NOTE: the predicate overrides the predexp
        """
        test_bin = 'Stpredold'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]

        predexp = [
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        number_predicate = predicates.equals('number', 4)

        policy = {
            'predexp': predexp
        }

        scan = self.as_connection.scan(TEST_NS, TEST_SET)
        scan.where(number_predicate)
        scan.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])
        job_id = scan.execute_background(policy)
        wait_for_job_completion(self.as_connection, job_id)

        for key in keys:
            _, _, bins = self.as_connection.get(key)
            if bins['number'] == 4:
                assert(bins[test_bin] == 'aerospike')
            else:
                assert(bins.get(test_bin) is None)

    def test_background_execute_with_ops_and_predexp(self):
        """
        Ensure that Scan.execute_background() applies ops to records that match the predexp.
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        test_bin = 'Stops_preds'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]

        scan = self.as_connection.scan(TEST_NS, TEST_SET)
        # scan.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])

        ops = [
            operations.write(test_bin, 'new_val')
        ]

        predexp = [
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        policy = {
            'predexp': predexp
        }

        scan.add_ops(ops)
        job_id = scan.execute_background(policy)
        wait_for_job_completion(self.as_connection, job_id)

        for key in keys:
            _, _, bins = self.as_connection.get(key)
            if bins['number'] == 2 or bins['number'] == 3:
                assert(bins[test_bin] == 'new_val')
            else:
                assert(bins.get(test_bin) is None)

    def test_background_execute_with_ops_and_predexp_None_set(self):
        """
        Ensure that Scan.execute_background() applies ops to all records in the NS that match the predexp.
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        test_bin = 'Stops_noset'
        keys = [(TEST_NS, TEST_SET, i) for i in range(50)]
        keys2 = [(TEST_NS, TEST_SET2, i) for i in range(10)]
        scan = self.as_connection.scan(TEST_NS, None)
        # scan.apply(TEST_UDF_MODULE, TEST_UDF_FUNCTION, [test_bin])

        ops = [
            operations.write(test_bin, 'new_val')
        ]

        predexp = [
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(2),
            as_predexp.integer_equal(),
            as_predexp.integer_bin('number'),
            as_predexp.integer_value(3),
            as_predexp.integer_equal(),
            as_predexp.predexp_or(2)
        ]

        policy = {
            'predexp': predexp
        }

        scan.add_ops(ops)
        job_id = scan.execute_background(policy)
        wait_for_job_completion(self.as_connection, job_id)

        for key in keys:
            _, _, bins = self.as_connection.get(key)
            if bins['number'] == 2 or bins['number'] == 3:
                assert(bins[test_bin] == 'new_val')
            else:
                assert(bins.get(test_bin) is None)
        
        for key in keys2:
            _, _, bins = self.as_connection.get(key)
            if bins['number'] == 2 or bins['number'] == 3:
                assert(bins[test_bin] == 'new_val')
            else:
                assert(bins.get(test_bin) is None)
