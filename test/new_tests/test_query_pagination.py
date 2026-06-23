# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
from aerospike import exception as e
import aerospike
from .as_status_codes import AerospikeStatus
import math


@pytest.mark.usefixtures("setup_many_records")
class TestQueryPagination(TestBaseClass):
    @pytest.mark.xfail(reason="Might fail, server may return less than what asked for.")
    def test_query_pagination_with_existent_ns_and_set(self):

        records = []
        query_page_size = [12]
        query_count = [0]
        query_pages = [5]
        max_records = (
            self.partition_1000_count
            + self.partition_1001_count
            + self.partition_1002_count
            + self.partition_1003_count
        )
        partition_filter = {"begin": 1000, "count": 4}
        policy = {"max_records": query_page_size[0], "partition_filter": partition_filter, "records_per_second": 4000}

        def callback(part_id, input_tuple):
            if input_tuple is None:
                return True  # query complete
            (_, _, record) = input_tuple
            records.append(record)
            query_count[0] = query_count[0] + 1

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        i = 0
        for i in range(query_pages[0]):
            query_obj.foreach(callback, policy)
            assert query_page_size[0] == query_count[0]
            query_count[0] = 0
            if query_obj.is_done() is True:
                # print(f"query completed iter:{i}")
                break

        assert len(records) == (
            (query_page_size[0] * query_pages[0])
            if (query_page_size[0] * query_pages[0]) < max_records
            else max_records
        )

    def test_query_pagination_with_existent_ns_and_none_set(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, None)
        query_obj.paginate()

        NUM_PARTITIONS = 4
        num_records_from_part_1000_to_1003 = (
            self.partition_1000_count
            + self.partition_1001_count
            + self.partition_1002_count
            + self.partition_1003_count
        )
        avg_records_per_partition = math.ceil(num_records_from_part_1000_to_1003 / NUM_PARTITIONS)
        query_obj.max_records = avg_records_per_partition

        NUM_ITERATIONS = NUM_PARTITIONS
        for _ in range(NUM_ITERATIONS):
            query_obj.foreach(
                callback,
                {
                    "partition_filter": {"begin": 1000, "count": NUM_PARTITIONS},
                },
            )

        # Worst case scenario, all the records are in one node
        # Best case scenario, we got all the records back
        assert NUM_ITERATIONS <= len(records) <= num_records_from_part_1000_to_1003

    # NOTE: This could fail if node record counts are small and unbalanced across nodes.
    @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_query_pagination_with_max_records_policy(self):

        records = []

        max_records = self.partition_1000_count

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        query_obj.foreach(callback, {"max_records": max_records, "partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) == self.partition_1000_count

    def test_query_pagination_with_results_method(self):

        ns = "test"
        st = "demo"
        all_recs = 0

        query_obj: aerospike.Query = self.as_connection.query(ns, st)

        query_obj.max_records = math.ceil(self.partition_1001_count / 2)

        part_filter = {"begin": 1001, "count": 1}

        for i in range(2):
            records = query_obj.results({"partition_filter": part_filter})
            all_recs += len(records)

        assert all_recs == self.partition_1001_count

        # Even though the client has queried all the records,
        # it doesn't know for sure it has read all the records until it queries one more time.
        query_obj.results({"partition_filter": part_filter})
        assert query_obj.is_done()

    def test_query_pagination_with_multiple_foreach_on_same_query_object(self):
        """
        Invoke multiple foreach on same query object.
        """
        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert len(records) == self.partition_1001_count

        records = []
        query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert len(records) == 0

    def test_query_pagination_with_multiple_results_call_on_same_query_object(self):

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        records = query_obj.results({"partition_filter": {"begin": 1002, "count": 1}})
        assert len(records) == self.partition_1002_count

        records = []
        records = query_obj.results({"partition_filter": {"begin": 1002, "count": 1}})
        assert len(records) == 0

    def test_query_pagination_without_any_parameter(self):

        with pytest.raises(e.ParamError):
            self.as_connection.query()
            assert True

    def test_query_pagination_with_non_existent_ns_and_set(self):

        ns = "namespace"
        st = "set"

        records = []
        query_obj = self.as_connection.query(ns, st)
        query_obj.paginate()

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.NamespaceNotFound) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_query_pagination_with_callback_contains_error(self):
        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            raise Exception("callback error")
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_query_pagination_with_callback_non_callable(self):

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(5, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_query_pagination_with_callback_wrong_number_of_args(self):
        def callback():
            pass

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT
