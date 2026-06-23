# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus


@pytest.mark.usefixtures("setup_many_records")
class TestScanPartition(TestBaseClass):
    def test_scan_partition_with_existent_ns_and_set(self):

        records = []
        partition_filter = {"begin": 1000, "count": 1}
        policy = {
            "max_retries": 100,
            "max_records": 1000,
            "partition_filter": partition_filter,
            "records_per_second": 4000,
        }

        def callback(part_id, input_tuple):
            (_, _, record) = input_tuple
            records.append(record)
            # print(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, policy)

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_existent_ns_and_none_set(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, None)

        scan_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_timeout_policy(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"total_timeout": 180000, "partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    # NOTE: This could fail if node record counts are small and unbalanced across nodes.
    @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_scan_partition_with_max_records_policy(self):

        records = []

        max_records = self.partition_1000_count // 2

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"max_records": max_records, "partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) == self.partition_1000_count // 2

    @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_scan_partition_with_all_records_policy(self):

        records = []

        max_records = (
            self.partition_1000_count
            + self.partition_1001_count
            + self.partition_1002_count
            + self.partition_1003_count
        )

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"max_records": max_records, "partition_filter": {"begin": 1000, "count": 4}})
        assert len(records) == max_records

    def test_scan_partition_with_socket_timeout_policy(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"socket_timeout": 180000, "partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_records_per_second_policy(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"records_per_second": 10, "partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_callback_returning_false(self):
        """
        Invoke scan() with callback function returns false
        """

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            if len(records) == 10:
                return False
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) == 10

    def test_scan_partition_with_results_method(self):

        ns = "test"
        st = "demo"

        scan_obj = self.as_connection.scan(ns, st)

        records = scan_obj.results({"partition_filter": {"begin": 1001, "count": 1}})
        assert len(records) == self.partition_1001_count

    def test_scan_partition_with_multiple_foreach_on_same_scan_object(self):
        """
        Invoke multiple foreach on same scan object.
        """
        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert len(records) == self.partition_1001_count

        records = []
        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)
        scan_obj2.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert len(records) == self.partition_1001_count

    def test_scan_partition_with_multiple_results_call_on_same_scan_object(self):

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({"partition_filter": {"begin": 1002, "count": 1}})
        assert len(records) == self.partition_1002_count

        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)
        records = []
        records = scan_obj2.results({"partition_filter": {"begin": 1002, "count": 1}})
        assert len(records) == self.partition_1002_count

    def test_scan_partition_without_any_parameter(self):

        with pytest.raises(e.ParamError):
            self.as_connection.scan()
            assert True

    def test_scan_partition_with_non_existent_ns_and_set(self):

        ns = "namespace"
        st = "set"

        records = []
        scan_obj = self.as_connection.scan(ns, st)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.NamespaceNotFound) as err_info:
            scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_scan_partition_with_callback_contains_error(self):
        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            raise Exception("callback error")
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_partition_with_callback_non_callable(self):
        # TODO
        records = []  # noqa: F841

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(5, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_partition_with_callback_wrong_number_of_args(self):
        def callback():
            pass

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_resume_part_scan(self):
        """
        Resume a scan using foreach.
        """
        records = 0
        resumed_records = 0

        def callback(part_id, input_tuple):
            nonlocal records
            if records == 5:
                return False
            records += 1

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert records == 5

        partition_status = scan_obj.get_partitions_status()

        def resume_callback(part_id, input_tuple):
            nonlocal resumed_records
            resumed_records += 1

        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)

        policy = {
            "partition_filter": {"begin": 1001, "count": 1, "partition_status": partition_status},
        }

        scan_obj2.foreach(resume_callback, policy)

        assert records + resumed_records == self.partition_1001_count

    def test_resume_scan_results(self):

        """
        Resume a scan using results.
        """
        records = 0

        def callback(part_id, input_tuple):
            nonlocal records
            if records == 5:
                return False
            records += 1

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert records == 5

        partition_status = scan_obj.get_partitions_status()

        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)

        policy = {
            "partition_filter": {"begin": 1001, "count": 1, "partition_status": partition_status},
        }

        results = scan_obj2.results(policy)

        assert records + len(results) == self.partition_1001_count
