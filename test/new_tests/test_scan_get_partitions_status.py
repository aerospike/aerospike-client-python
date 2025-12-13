# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass


@pytest.fixture(autouse=True, scope="class")
def setup(request, as_connection):
    request.cls.test_ns = "test"
    request.cls.test_set = "demo"

    request.cls.partition_1000_count = 0
    request.cls.partition_1001_count = 0
    request.cls.partition_1002_count = 0
    request.cls.partition_1003_count = 0

    as_connection.truncate(request.cls.test_ns, None, 0)

    for i in range(1, 100000):
        put = 0
        key = (request.cls.test_ns, request.cls.test_set, str(i))
        rec_partition = as_connection.get_key_partition_id(request.cls.test_ns, request.cls.test_set, str(i))

        if rec_partition == 1000:
            request.cls.partition_1000_count += 1
            put = 1
        if rec_partition == 1001:
            request.cls.partition_1001_count += 1
            put = 1
        if rec_partition == 1002:
            request.cls.partition_1002_count += 1
            put = 1
        if rec_partition == 1003:
            request.cls.partition_1003_count += 1
            put = 1
        if put:
            rec = {
                "i": i,
                "s": "xyz",
                "l": [2, 4, 8, 16, 32, None, 128, 256],
                "m": {"partition": rec_partition, "b": 4, "c": 8, "d": 16},
            }
            as_connection.put(key, rec)

    def teardown():
        for i in range(1, 100000):
            put = 0
            key = ("test", "demo", str(i))
            rec_partition = as_connection.get_key_partition_id(request.cls.test_ns, request.cls.test_set, str(i))

            if rec_partition == 1000:
                request.cls.partition_1000_count += 1
                put = 1
            if rec_partition == 1001:
                request.cls.partition_1001_count += 1
                put = 1
            if rec_partition == 1002:
                request.cls.partition_1002_count += 1
                put = 1
            if rec_partition == 1003:
                request.cls.partition_1003_count += 1
                put = 1
            if put:
                as_connection.remove(key)

    request.addfinalizer(teardown)


class TestScanGetPartitionsStatus(TestBaseClass):
    def test_scan_get_partitions_status_no_tracking(self):
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        stats = scan_obj.get_partitions_status()
        assert stats == {}

    def test_get_partitions_status_after_foreach(self):
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

    def test_scan_get_partitions_status_results(self):
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        # policy = {'partition_filter': {'begin': 1001, 'count': 1}}
        scan_obj.paginate()
        scan_obj.results()

        stats = scan_obj.get_partitions_status()
        assert stats

    def test_scan_get_partitions_status_results_no_tracking(self):
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        # policy = {'partition_filter': {'begin': 1001, 'count': 1}}
        scan_obj.results()

        stats = scan_obj.get_partitions_status()
        assert not stats

    def test_scan_get_partitions_status_results_parts(self):
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        policy = {"partition_filter": {"begin": 1001, "count": 1}}
        results = scan_obj.results(policy)
        assert len(results) == self.partition_1001_count

        stats = scan_obj.get_partitions_status()
        assert stats

    def test_scan_get_partitions_status_foreach_parts(self):
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        ids = []

        def callback(part_id, input_tuple):
            ids.append(part_id)

        policy = {"partition_filter": {"begin": 1001, "count": 1}}
        scan_obj.foreach(callback, policy)
        assert len(ids) == self.partition_1001_count

        stats = scan_obj.get_partitions_status()
        assert stats
