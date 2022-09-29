# -*- coding: utf-8 -*-
import pytest
import sys
from .as_status_codes import AerospikeStatus
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestScanInfo(object):

    udf_to_load = "bin_lua.lua"

    # connection_with_udf will remove the udf at the end of the tests
    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_udf):
        """
        Setup method.
        """
        for i in range(15):
            key = ('test', 'demo', i)
            rec = {'age': i}
            connection_with_udf.put(key, rec)
        policy = {}
        self.job_id = connection_with_udf.scan_apply(
            "test", "demo", "bin_lua", "mytransform", ['age', 2])

        def teardown():
            """
            Teardown method.
            """
            for i in range(15):
                key = ('test', 'demo', i)
                connection_with_udf.remove(key)

        request.addfinalizer(teardown)

    def test_job_info_with_no_parameters(self):
        """
        Invoke job_info() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.job_info()
        assert "argument 'job_id' (pos 1)" in str(
            typeError.value)

    @pytest.mark.xfail(reason="This test fails if job_info() finishes in < 1ms")
    def test_job_info_with_small_timeout(self, connection_with_udf):
        """
        Invoke job_info() with correct policy and an expected timeout
        """
        policy = {'timeout': 1}

        self.job_id = connection_with_udf.scan_apply(
            "test", "demo", "bin_lua", "mytransform", ['age', 2], block=False)

        with pytest.raises(e.TimeoutError):
            job_info = self.as_connection.job_info(
                self.job_id, aerospike.JOB_SCAN, policy)

    def test_job_info_with_correct_parameters(self):
        """
        Invoke job_info() with correct parameters
        """
        job_info = self.as_connection.job_info(self.job_id, aerospike.JOB_SCAN)
        valid_statuses = (
            aerospike.JOB_STATUS_COMPLETED,
            aerospike.JOB_STATUS_INPROGRESS
        )
        assert job_info['status'] in valid_statuses

        expected_fields = ('status', 'progress_pct', 'records_read')
        # Make sure that the fields we are expected are in the returned
        # dict
        for field in expected_fields:
            assert field in job_info

    def test_job_info_with_correct_policy(self):
        """
        Invoke job_info() with correct policy
        """
        policy = {'timeout': 1000}
        job_info = self.as_connection.job_info(
            self.job_id, aerospike.JOB_SCAN, policy)

        valid_statuses = (
            aerospike.JOB_STATUS_COMPLETED,
            aerospike.JOB_STATUS_INPROGRESS
        )
        assert job_info['status'] in valid_statuses

    def test_job_info_with_incorrect_policy(self):
        """
        Invoke job_info() with incorrect policy
        """

        policy = {'timeout': 0.5}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.job_info(
                self.job_id, aerospike.JOB_SCAN, policy)

        assert err_info.value.code == -2
        assert err_info.value.msg == "timeout is invalid"

    def test_job_info_with_scanid_incorrect(self):
        """
        Invoke job_info() with scan id incorrect,
        this should not raise an error
        """
        response = self.as_connection.job_info(
            self.job_id + 2, aerospike.JOB_SCAN)

        assert response['status'] == aerospike.JOB_STATUS_COMPLETED

    def test_job_info_with_largeid(self):
        """
        Invoke job_info() with a large scan id,
        this should not raise an error
        """
        response = self.as_connection.job_info(
            13287138843617152748, aerospike.JOB_SCAN)

        assert response['status'] == aerospike.JOB_STATUS_COMPLETED

    def test_job_info_with_scanid_string(self):
        """
        Invoke job_info() with scan id incorrect
        """

        with pytest.raises(TypeError) as typeError:
            self.as_connection.job_info("string")
        assert(any(["job_info() argument 1 must be int" in str(typeError.value),
         "job_info() argument 1 must be an int" in str(typeError.value)]))

    def test_job_info_with_correct_parameters_without_connection(self):
        """
        Invoke job_info() with correct parameters without connection
        """

        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.job_info(self.job_id, aerospike.JOB_SCAN)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_job_info_with_constant_out_of_valid_values(self):
        """
        Invoke job_info() with the scan module out of the expected range
        """
        with pytest.raises(e.ParamError):
            response = self.as_connection.job_info(
                self.job_id, "not query nor scan")

    @pytest.mark.parametrize(
        "module",
        (None, 1.5, {}, [], 0)
    )
    def test_job_info_with_module_wrong_type(self, module):
        """
        Invoke job_info() with the scan module argument of the wrong type
        """
        with pytest.raises(TypeError):
            response = self.as_connection.job_info(
                self.job_id, module)
