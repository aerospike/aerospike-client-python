# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
from aerospike import exception as e
import aerospike
from .as_status_codes import AerospikeStatus


class TestQueryPagination(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support paginated queries.")
            pytest.xfail()
        self.test_ns = "test"
        self.test_set = "demo"

        self.partition_1000_count = 0
        self.partition_1001_count = 0
        self.partition_1002_count = 0
        self.partition_1003_count = 0

        as_connection.truncate(self.test_ns, None, 0)

        for i in range(1, 100000):
            put = 0
            rec_partition = as_connection.get_key_partition_id(self.test_ns, self.test_set, str(i))

            if rec_partition == 1000:
                self.partition_1000_count += 1
                put = 1
            if rec_partition == 1001:
                self.partition_1001_count += 1
                put = 1
            if rec_partition == 1002:
                self.partition_1002_count += 1
                put = 1
            if rec_partition == 1003:
                self.partition_1003_count += 1
                put = 1
            if put:
                rec = {
                    "i": i,
                    "s": "xyz",
                    "l": [2, 4, 8, 16, 32, None, 128, 256],
                    "m": {"partition": rec_partition, "b": 4, "c": 8, "d": 16},
                }
                key = {
                    "ns": self.test_ns,
                    "set": self.test_set,
                    "key": str(i),
                    "digest": aerospike.calc_digest(self.test_ns, self.test_set, str(i)),
                }
                as_connection.put(key, rec)

        def teardown():
            for i in range(1, 100000):
                put = 0
                key = ("test", "demo", str(i))
                rec_partition = as_connection.get_key_partition_id(self.test_ns, self.test_set, str(i))

                if rec_partition == 1000:
                    self.partition_1000_count += 1
                    put = 1
                if rec_partition == 1001:
                    self.partition_1001_count += 1
                    put = 1
                if rec_partition == 1002:
                    self.partition_1002_count += 1
                    put = 1
                if rec_partition == 1003:
                    self.partition_1003_count += 1
                    put = 1
                if put:
                    as_connection.remove(key)

        request.addfinalizer(teardown)

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

        num_populated_partitions = 4
        all_records = (
            self.partition_1000_count
            + self.partition_1001_count
            + self.partition_1002_count
            + self.partition_1003_count
        )
        self.partition_1000_count / num_populated_partitions

        for i in range(num_populated_partitions):
            query_obj.foreach(
                callback,
                {
                    "partition_filter": {"begin": 1000, "count": num_populated_partitions},
                    "max_records": all_records / num_populated_partitions,
                },
            )

        assert len(records) == all_records

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

        query_obj = self.as_connection.query(ns, st)

        max_records = self.partition_1001_count / 2

        for i in range(2):
            records = query_obj.results({"partition_filter": {"begin": 1001, "count": 1}, "max_records": max_records})

            all_recs += len(records)

        assert all_recs == self.partition_1001_count
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
