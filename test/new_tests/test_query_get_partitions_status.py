# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike


class TestQueryGetPartitionsStatus(TestBaseClass):
    @pytest.mark.parametrize("query_type", [aerospike.Client.scan, aerospike.Client.query])
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, query_type):
        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support partition queries.")
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
            key = (self.test_ns, self.test_set, str(i))
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
                as_connection.put(key, rec)

        self.query_creation_method = query_type

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

    def test_query_get_partitions_status_foreach_one_partition(self):
        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()
        ids = []

        def callback(part_id, input_tuple):
            ids.append(part_id)

        policy = {"partition_filter": {"begin": 1001, "count": 1}}
        query_obj.foreach(callback, policy)
        assert len(ids) == self.partition_1001_count

        stats = query_obj.get_partitions_status()
        assert type(stats) == aerospike.PartitionsStatus

    def test_get_partitions_status_terminate_resume(self):
        """
        Resume a query using foreach.
        """
        records = 0
        resumed_records = 0

        def callback(part_id, input_tuple):
            nonlocal records
            if records == 5:
                return False
            records += 1

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.paginate()
        query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert records == 5

        partition_status = query_obj.get_partitions_status()
        assert type(partition_status) == aerospike.PartitionsStatus

        def resume_callback(part_id, input_tuple):
            nonlocal resumed_records
            resumed_records += 1

        query_obj2 = self.as_connection.query(self.test_ns, self.test_set)

        policy = {
            "partition_filter": {"begin": 1001, "count": 1, "partition_status": partition_status},
        }

        query_obj2.foreach(resume_callback, policy)
        assert records + resumed_records == self.partition_1001_count

    @pytest.mark.parametrize("policy", [
        {},
        {"partition_filter": {"begin": 1001, "count": 1}}
    ])
    def test_query_get_partitions_status_results(self, policy):
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        query_obj.paginate()
        results = query_obj.results(policy)

        stats = query_obj.get_partitions_status()
        assert type(stats) == aerospike.PartitionsStatus

        if "partition_filter" in policy:
            assert len(results) == self.partition_1001_count

    # Negative tests

    def test_query_get_partitions_status_no_tracking(self):
        # Non-paginated queries don't support getting partitions status
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        stats = query_obj.get_partitions_status()
        assert stats is None

    def test_query_get_partitions_status_results_no_tracking(self):
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        # policy = {'partition_filter': {'begin': 1001, 'count': 1}}
        query_obj.results()

        stats = query_obj.get_partitions_status()
        assert stats is None
