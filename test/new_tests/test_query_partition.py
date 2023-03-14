# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass

from aerospike import exception as e
from aerospike_helpers import expressions as exp
from .as_status_codes import AerospikeStatus
from aerospike import predicates as p


import aerospike


def add_sindex(client):
    """
    Load the sindex used in the tests
    """
    try:
        client.index_string_create("test", "demo", "s", "string")
    except e.IndexFoundError:
        pass


def remove_sindex(client):
    """
    Remove the sindex created for these tests
    """
    try:
        client.index_remove("test", "string", {})
    except e.IndexNotFound:
        pass


class TestQueryPartition(TestBaseClass):
    def setup_class(cls):
        # Register setup and teardown functions
        cls.connection_setup_functions = [add_sindex]
        cls.connection_teardown_functions = [remove_sindex]

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support partition queries.")
            pytest.xfail()
        as_connection = connection_with_config_funcs

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
        # print(f"{self.partition_1000_count} records are put in partition 1000, \
        #         {self.partition_1001_count} records are put in partition 1001, \
        #         {self.partition_1002_count} records are put in partition 1002, \
        #         {self.partition_1003_count} records are put in partition 1003")

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

    def test_query_partition_with_existent_ns_and_set(self):

        records = []
        partition_filter = {"begin": 1000, "count": 1}
        policy = {"max_retries": 100, "partition_filter": partition_filter}

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = 1000
        query_obj.records_per_second = 4000

        query_obj.foreach(callback, policy)

        assert len(records) == self.partition_1000_count

    def test_query_partition_with_where(self):

        records = []
        partition_filter = {"begin": 1000, "count": 1}
        policy = {"max_retries": 100, "partition_filter": partition_filter}

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = 1000
        query_obj.records_per_second = 4000
        query_obj.where(p.equals("s", "xyz"))

        query_obj.foreach(callback, policy)

        assert len(records) == self.partition_1000_count

        part_stats = query_obj.get_partitions_status()

        bval = part_stats[1000][4]
        assert bval != 0

    def test_query_partition_with_short_query(self):

        records = []
        partition_filter = {"begin": 1000, "count": 1}
        policy = {"max_retries": 100, "partition_filter": partition_filter, "short_query": True}

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = 100
        query_obj.where(p.equals("s", "xyz"))

        query_obj.foreach(callback, policy)

        assert len(records) == self.partition_1000_count and len(records) <= 100

    def test_query_partition_with_filter(self):

        records = []

        partition_filter = {"begin": 1000, "count": 4}

        expr = exp.Eq(
            exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.INTEGER, "partition", exp.MapBin("m")),
            1002,
        )

        policy = {"max_retries": 100, "expressions": expr.compile(), "partition_filter": partition_filter}

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = 1000
        query_obj.records_per_second = 4000

        query_obj.foreach(callback, policy)

        assert len(records) == self.partition_1002_count

    def test_query_partition_with_filter_zero(self):

        records = []

        partition_filter = {"begin": 1000, "count": 1}

        expr = exp.Eq(
            exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.INTEGER, "partition", exp.MapBin("m")),
            1002,
        )

        policy = {"max_retries": 100, "expressions": expr.compile(), "partition_filter": partition_filter}

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = 1000
        query_obj.records_per_second = 4000

        query_obj.foreach(callback, policy)

        assert len(records) == 0

    def test_query_partition_with_existent_ns_and_none_set(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, None)

        query_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    def test_query_partition_with_timeout_policy(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        query_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    # NOTE: This could fail if node record counts are small and unbalanced across nodes.
    # @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_query_partition_with_max_records_policy(self):

        records = []

        max_records = self.partition_1000_count // 2

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = max_records

        query_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) <= self.partition_1000_count // 2

    # NOTE: This could fail if node record counts are small and unbalanced across nodes.
    # @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_query_partition_with_all_records_policy(self):

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

        query_obj = self.as_connection.query(self.test_ns, self.test_set)
        query_obj.max_records = max_records

        query_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 4}})
        assert len(records) <= max_records

    def test_query_partition_with_socket_timeout_policy(self):

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        query_obj.foreach(callback, {"socket_timeout": 180000, "partition_filter": {"begin": 1000, "count": 1}})

        assert len(records) == self.partition_1000_count

    def test_query_partition_with_callback_returning_false(self):
        """
        Invoke query() with callback function returns false
        """

        records = []

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            if len(records) == 10:
                return False
            records.append(record)

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        query_obj.foreach(callback, {"partition_filter": {"begin": 1000, "count": 1}})
        assert len(records) == 10

    def test_query_partition_with_results_method(self):

        ns = "test"
        st = "demo"

        query_obj = self.as_connection.query(ns, st)

        records = query_obj.results({"partition_filter": {"begin": 1001, "count": 1}})
        assert len(records) == self.partition_1001_count

    def test_resume_part_query(self):
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

        query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 2}})

        assert records == 5

        partition_status = query_obj.get_partitions_status()

        def resume_callback(part_id, input_tuple):
            nonlocal resumed_records
            resumed_records += 1

        query_obj2 = self.as_connection.query(self.test_ns, self.test_set)

        policy = {
            "partition_filter": {"begin": 1001, "count": 2, "partition_status": partition_status},
        }

        query_obj2.foreach(resume_callback, policy)

        assert records + resumed_records == self.partition_1001_count + self.partition_1002_count

    def test_resume_query_results(self):

        """
        Resume a query using results.
        """
        records = 0

        def callback(part_id, input_tuple):
            nonlocal records
            if records == 5:
                return False
            records += 1

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        assert records == 5

        partition_status = query_obj.get_partitions_status()

        query_obj2 = self.as_connection.query(self.test_ns, self.test_set)

        policy = {
            "partition_filter": {"begin": 1001, "count": 1, "partition_status": partition_status},
        }

        results = query_obj2.results(policy)

        assert records + len(results) == self.partition_1001_count

    def test_query_partition_with_non_existent_ns_and_set(self):

        ns = "namespace"
        st = "set"

        records = []
        query_obj = self.as_connection.query(ns, st)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.NamespaceNotFound) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_query_partition_with_callback_contains_error(self):
        def callback(part_id, input_tuple):
            raise Exception("callback error")

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_query_partition_with_callback_non_callable(self):
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(5, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT  # TODO this should be an err param

    def test_query_partition_with_callback_wrong_number_of_args(self):
        def callback(input_tuple):
            pass

        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            query_obj.foreach(callback, {"partition_filter": {"begin": 1001, "count": 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    @pytest.mark.parametrize(
        "begin",
        [
            (-1),
            (4096),
        ],
    )
    def test_query_partition_with_bad_begin(self, begin):
        records = []
        policy = {"partition_filter": {"begin": begin, "count": 1}}
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.ParamError) as err_info:
            query_obj.foreach(callback, policy)
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
        assert "invalid partition_filter policy begin" in err_info.value.msg

    @pytest.mark.parametrize(
        "count",
        [
            (-1),
            (4097),
        ],
    )
    def test_query_partition_with_bad_count(self, count):
        records = []
        policy = {"partition_filter": {"begin": 0, "count": count}}
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.ParamError) as err_info:
            query_obj.foreach(callback, policy)
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
        assert "invalid partition_filter policy count" in err_info.value.msg

    @pytest.mark.parametrize(
        "begin, count",
        [
            (500, 4096),
            (4095, 2),
        ],
    )
    def test_query_partition_with_bad_range(self, begin, count):
        records = []
        policy = {"partition_filter": {"begin": begin, "count": count}}
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.ParamError) as err_info:
            query_obj.foreach(callback, policy)
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
        assert "invalid partition filter range" in err_info.value.msg

    @pytest.mark.parametrize(
        "p_stats, expected, msg",
        [
            (
                {
                    "done": False,
                    "retry": True,
                    1001: (1001, True, False, bytearray(b"\xe9\xe31\x01sS\xedafw\x00W\xcdM\x80\xd0L\xee\\d"), 0),
                    1002: (
                        1002,
                        "bad_init",
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "invalid init for part_id: 1002",
            ),
            (
                {
                    "done": False,
                    "retry": True,
                    1002: (
                        1002,
                        False,
                        "bad_done",
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "invalid retry for part_id: 1002",
            ),
            (
                {"done": False, "retry": True, 1003: (1003, False, False, "bad_digest", 0)},
                e.ParamError,
                "invalid digest value for part_id: 1003",
            ),
            (
                {
                    "done": False,
                    "retry": True,
                    1004: (
                        1004,
                        False,
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        "bad_bval",
                    ),
                },
                e.ParamError,
                "invalid bval for part_id: 1004",
            ),
            (
                {
                    "done": "bad_done",
                    "retry": True,
                    1004: (
                        1004,
                        False,
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "partition_status dict key 'done' must be an int",
            ),
            (
                {
                    "done": False,
                    "retry": "bad_retry",
                    1004: (
                        1004,
                        False,
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "partition_status dict key 'retry' must be an int",
            ),
            (
                {
                    "retry": True,
                    1004: (
                        1004,
                        False,
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "partition_status dict missing key 'done'",
            ),
            (
                {
                    "done": False,
                    1004: (
                        1004,
                        False,
                        False,
                        bytearray(b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
                        0,
                    ),
                },
                e.ParamError,
                "partition_status dict missing key 'retry'",
            ),
        ],
    )
    def test_query_partition_with_bad_status(self, p_stats, expected, msg):
        records = []
        policy = {"partition_filter": {"begin": 1000, "count": 5, "partition_status": p_stats}}
        query_obj = self.as_connection.query(self.test_ns, self.test_set)

        def callback(part_id, input_tuple):
            _, _, record = input_tuple
            records.append(record)

        # query_obj.foreach(callback, policy)
        with pytest.raises(expected) as exc:
            query_obj.foreach(callback, policy)

        assert msg in exc.value.msg
