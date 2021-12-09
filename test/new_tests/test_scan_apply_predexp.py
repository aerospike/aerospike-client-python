# -*- coding: utf-8 -*-
import pytest
import time
import sys
from .as_status_codes import AerospikeStatus
from aerospike import predexp as as_predexp
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


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


class TestScanApply(object):

    def setup_class(cls):
        cls.udf_to_load = u"bin_lua.lua"
        cls.loaded_udf_name = "bin_lua.lua"
        cls.udf_language = aerospike.UDF_TYPE_LUA

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_udf):
        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            connection_with_udf.put(key, rec)

        no_set_key = ('test', None, 'no_set')
        rec = {'name': 'no_ns_key', 'age': 10}
        connection_with_udf.put(no_set_key, rec)

        def teardown():
            for i in range(5):
                key = ('test', 'demo', i)
                connection_with_udf.remove(key)
            connection_with_udf.remove(no_set_key)
        request.addfinalizer(teardown)

    def test_scan_apply_with_correct_policy_and_predexp(self):
        """
        Invoke scan_apply() with correct policy.
        It should invoke the function on all records in the set that match the predexp.
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value('name4'),
            as_predexp.string_equal(),
            as_predexp.integer_bin('age'),
            as_predexp.integer_value(3),
            as_predexp.integer_unequal(),
            as_predexp.predexp_and(2)
        ]

        policy = {'timeout': 1000, 'predexp': predexp}
        scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'name4':
                assert bins['age'] == i + 2
            else :
                assert bins['age'] == i

    def test_scan_apply_with_correct_policy_and_invalid_predexp(self):
        """
        Invoke scan_apply() with invalid predexp.
        """
        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value(4),
            as_predexp.string_equal(),
        ]

        policy = {'timeout': 1000, 'predexp': predexp}
        with pytest.raises(e.ParamError):
            scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                    "mytransform", ['age', 2],
                                                    policy)

    def test_scan_apply_with_none_set_and_predexp(self):
        """
        Invoke scan_apply() with set argument as None
        It should invoke the function on all records in NS that match the predexp
        """
        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value('name2'),
            as_predexp.string_equal(),
            as_predexp.integer_bin('age'),
            as_predexp.integer_value(3),
            as_predexp.integer_unequal(),
            as_predexp.predexp_and(2)
        ]

        policy = {'timeout': 1000, 'predexp': predexp}
        scan_id = self.as_connection.scan_apply("test", None, "bin_lua",
                                                "mytransform", ['age', 2],
                                                policy)

        wait_for_job_completion(self.as_connection, scan_id)

        for i in range(5):
            key = ('test', 'demo', i)
            _, _, bins = self.as_connection.get(key)
            if bins['name'] == 'name2':
                assert bins['age'] == i + 2
            else :
                assert bins['age'] == i

        _, _, rec = self.as_connection.get(('test', None, 'no_set'))
        assert rec['age'] == 10
