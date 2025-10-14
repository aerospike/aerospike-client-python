# -*- coding: utf-8 -*-
import pytest
from aerospike import exception as e
from aerospike_helpers.operations import hll_operations
from aerospike_helpers import HyperLogLog
from math import sqrt


import aerospike


class TestHLL(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_keys = []
        for x in range(5):

            key = ("test", "demo", x)
            record = {"label": x}

            as_connection.put(key, record)

            ops = [
                hll_operations.hll_init("hll_bine", 10),
                hll_operations.hll_add("hll_bin", ["key%s" % str(i) for i in range(x + 1)], 8),
                hll_operations.hll_add("mh_bin", ["key%s" % str(i) for i in range(x + 1)], 6, 12),
                hll_operations.hll_add("hll_binl", ["key%s" % str(i) for i in range(100)], 8),
                hll_operations.hll_add("hll_binlh", ["key%s" % str(i) for i in range(50)], 8),
                hll_operations.hll_add("hll_bin_big", ["key%s" % str(i) for i in range(1000)], 10),
                hll_operations.hll_add("hll_binu", ["key6", "key7", "key8", "key9", "key10"], 10),
            ]

            self.test_keys.append(key)
            _, _, _ = as_connection.operate(key, ops)

        def teardown():
            """
            Teardown method.
            """
            for key in self.test_keys:
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    def relative_count_error(self, n_index_bits):
        return 1.04 / sqrt(2**n_index_bits)

    def relative_intersect_error(self, n_index_bits, bin_counts, bin_intersect_count):
        sigma = 1.04 / sqrt(2**n_index_bits)
        rel_err_sum = 0
        for count in bin_counts:
            rel_err_sum += (sigma * count) ** 2
        rel_err_sum += sigma * (bin_intersect_count**2)

        return sqrt(rel_err_sum)

    def assert_within_error_bounds(self, vut, actual, error_percent):
        rel_error = vut * error_percent
        assert vut >= actual - rel_error and vut <= actual + rel_error

    @pytest.mark.parametrize(
        "policy, expected_result",
        [(None, 3), ({"flags": aerospike.HLL_WRITE_CREATE_ONLY}, 3), ({"flags": aerospike.HLL_WRITE_NO_FAIL}, 3)],
    )
    def test_pos_hll_add(self, policy, expected_result):
        """
        Invoke hll_add() creating a new HLL.
        """
        ops = [hll_operations.hll_add("new_bin", ["key1", "key2", "key3"], index_bit_count=8, policy=policy)]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)

        assert res["new_bin"] == expected_result

    @pytest.mark.parametrize(
        "policy, expected_result", [({"flags": aerospike.HLL_WRITE_UPDATE_ONLY}, e.InvalidRequest)]
    )
    def test_neg_hll_add(self, policy, expected_result):
        """
        Invoke hll_add() creating with expected failures.
        """
        ops = [hll_operations.hll_add("new_bin", ["key1", "key2", "key3"], index_bit_count=8, policy=policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[0], ops)

    def test_pos_hll_add_mh(self):
        """
        Invoke hll_add() creating a new min hash HLL.
        """
        ops = [hll_operations.hll_add("new_bin", ["key1", "key2", "key3"], 6, 8)]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)

        assert res["new_bin"] == 3

    @pytest.mark.parametrize(
        "policy, expected_result", [({"flags": aerospike.HLL_WRITE_CREATE_ONLY}, e.BinExistsError)]
    )
    def test_neg_hll_add_mh(self, policy, expected_result):
        """
        Invoke hll_add() failing to creating a new min hash HLL.
        """
        ops = [hll_operations.hll_add("hll_binl", ["key101", "key102", "key103"], 6, 8, policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[0], ops)

    def test_pos_hll_get_count(self):
        """
        Invoke hll_get_count() to check an HLL's count.
        """
        ops = [hll_operations.hll_get_count("hll_bin_big")]
        actual_count = 1000
        rel_error = self.relative_count_error(10)

        _, _, res = self.as_connection.operate(self.test_keys[2], ops)
        self.assert_within_error_bounds(res["hll_bin_big"], actual_count, rel_error)

    def test_neg_hll_get_count(self):
        """
        Invoke hll_get_count() with a non existent bin.
        """
        ops = [hll_operations.hll_get_count("fake_hll_bin")]

        _, _, res = self.as_connection.operate(self.test_keys[2], ops)
        assert res["fake_hll_bin"] is None

    def test_pos_hll_describe(self):
        """
        Invoke hll_describe() and check index and min hash bits.
        """
        ops = [hll_operations.hll_describe("mh_bin")]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)
        assert res["mh_bin"] == [6, 12]

    def test_neg_hll_describe(self):
        """
        Invoke hll_describe() with a non existent hll bin.
        """
        ops = [hll_operations.hll_describe("fake_hll_bin")]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)
        assert res["fake_hll_bin"] is None

    def test_pos_hll_fold(self):
        """
        Invoke hll_fold().
        """
        ops = [hll_operations.hll_fold("hll_bin", 6), hll_operations.hll_describe("hll_bin")]

        _, _, res = self.as_connection.operate(self.test_keys[2], ops)
        assert res["hll_bin"] == [6, 0]

    @pytest.mark.parametrize(
        "index_bits, bin, expected_result", [(2, "hll_bin", e.InvalidRequest), (4, "mh_bin", e.OpNotApplicable)]
    )
    def test_neg_hll_fold(self, index_bits, bin, expected_result):
        """
        Invoke hll_fold() expecting errors.
        """
        ops = [hll_operations.hll_fold(bin, index_bits), hll_operations.hll_describe(bin)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[2], ops)

    def test_pos_hll_get_intersect_count(self):
        """
        Invoke hll_get_intersect_count().
        """

        hll_bin_values = [br.record[2]["hll_binu"] for br in
                          self.as_connection.batch_read(self.test_keys[:1]).batch_records]
        ops = [hll_operations.hll_get_intersect_count("hll_binl", hll_bin_values)]
        rel_error = self.relative_intersect_error(8, [100, 5], 5)
        actual_intersect = 5

        _, _, res = self.as_connection.operate(self.test_keys[4], ops)
        assert res["hll_binl"] >= actual_intersect - rel_error and res["hll_binl"] <= actual_intersect + rel_error

    def test_pos_hll_get_intersect_count_mh(self):
        """
        Invoke hll_get_intersect_count() on min hash bins.
        """
        hll_bins = [br.record[2]["mh_bin"] for br in self.as_connection.batch_read(self.test_keys[:4]).batch_records]

        ops = [hll_operations.hll_get_intersect_count("mh_bin", hll_bins)]

        _, _, res = self.as_connection.operate(self.test_keys[4], ops)
        assert res["mh_bin"] == 1

    @pytest.mark.parametrize("hll_bins, bin, expected_result", [(5, "hll_binl", e.InvalidRequest)])
    def test_neg_hll_get_intersect_count(self, hll_bins, bin, expected_result):
        """
        Invoke hll_get_intersect_count() with expected failures.
        """

        hll_bins = [br.record[2][bin] for br in
                    self.as_connection.batch_read(self.test_keys[:hll_bins - 1]).batch_records]

        ops = [hll_operations.hll_get_intersect_count(bin, hll_bins)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[4], ops)

    def test_pos_hll_get_similarity(self):
        """
        Invoke hll_get_similarity().
        """

        hll = self.as_connection.get(self.test_keys[0])[2]["hll_binl"]

        ops = [hll_operations.hll_get_similarity("hll_binlh", [hll])]
        rel_error = self.relative_intersect_error(8, [100, 50], 50)
        actual_similarity = 0.50

        _, _, res = self.as_connection.operate(self.test_keys[4], ops)
        assert res["hll_binlh"] >= actual_similarity - rel_error and res["hll_binlh"] <= actual_similarity + rel_error

    @pytest.mark.parametrize("hll_bins, bin, expected_result", [(5, "hll_binl", e.InvalidRequest)])
    def test_neg_hll_get_similarity(self, hll_bins, bin, expected_result):
        """
        Invoke hll_get_similarity() with expected errors.
        """

        hll_bin_values = [br.record[2][bin] for br in
                          self.as_connection.batch_read(self.test_keys[: hll_bins - 1]).batch_records]

        ops = [hll_operations.hll_get_similarity(bin, hll_bin_values)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[4], ops)

    def test_pos_hll_get_union(self):
        """
        Invoke hll_get_union().
        """

        hll_bin_values = [br.record[2]["hll_binu"] for br in
                          self.as_connection.batch_read(self.test_keys).batch_records]

        ops = [hll_operations.hll_get_union("hll_bin", [hll_bin_values[4]])]

        _, _, union_hll = self.as_connection.operate(self.test_keys[4], ops)

        ops = [
            hll_operations.hll_get_intersect_count("hll_bin", [union_hll["hll_bin"]]),
            hll_operations.hll_get_intersect_count("hll_binu", [union_hll["hll_bin"]]),
        ]

        _, _, res = self.as_connection.operate(self.test_keys[4], ops)

        assert res["hll_bin"] == 5
        assert res["hll_binu"] == 5

    def test_neg_hll_get_union(self):
        """
        Invoke hll_get_union() with expected errors.
        """
        ops = [hll_operations.hll_get_union("hll_bin", [])]

        with pytest.raises(e.InvalidRequest):
            _, _, union_hll = self.as_connection.operate(self.test_keys[4], ops)

    def test_pos_hll_get_union_count(self):
        """
        Invoke hll_get_union_count().
        """

        hll_bin_values = [br.record[2]["hll_bin"] for br in self.as_connection.batch_read(self.test_keys).batch_records]

        ops = [hll_operations.hll_get_union_count("hll_binu", hll_bin_values)]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)

        assert res["hll_binu"] == 10

    def test_neg_hll_get_union_count(self):
        """
        Invoke hll_get_union_count() with errors.
        """

        records = []

        ops = [hll_operations.hll_get_union_count("hll_binu", records)]

        with pytest.raises(e.InvalidRequest):
            self.as_connection.operate(self.test_keys[0], ops)

    @pytest.mark.parametrize(
        "index_bits, mh_bits, bin, policy",
        [
            (4, 4, "new_mhbin", None),
            (4, 51, "new_mhbin", None),
            (9, 51, "new_mhbin", None),
            (6, 8, "new_mhbin", {"flags": aerospike.HLL_WRITE_CREATE_ONLY}),
            (6, 8, "new_mhbin", {"flags": aerospike.HLL_WRITE_DEFAULT}),
            (6, 8, "mh_bin", {"flags": aerospike.HLL_WRITE_UPDATE_ONLY}),
        ],
    )
    def test_pos_hll_init_mh(self, index_bits, mh_bits, bin, policy):
        """
        Invoke hll_init_mh() creating a new min hash HLL.
        """
        ops = [hll_operations.hll_init(bin, index_bits, mh_bits, policy)]

        _, _, _ = self.as_connection.operate(self.test_keys[0], ops)

        record = self.as_connection.get(self.test_keys[0])
        assert record[2][bin]

    @pytest.mark.parametrize(
        "index_bits, mh_bits, bin, policy, expected_result",
        [
            (0, 0, "new_mhbin", None, e.InvalidRequest),
            ("bad_ind_bits", 4, "new_mhbin", None, e.ParamError),
            (4, "bad_mh_bits", "new_mhbin", None, e.ParamError),
            (4, 4, "new_mhbin", "bad_policy", e.ParamError),
            (4, 4, ["bad_bin"], None, e.ParamError),
            (4, 52, "new_mhbin", None, e.InvalidRequest),
            (4, 59, "new_mhbin", None, e.InvalidRequest),
            (17, 48, "new_mhbin", None, e.InvalidRequest),
            (20, 8, "new_mhbin", None, e.InvalidRequest),
            (6, 8, "mh_bin", {"flags": aerospike.HLL_WRITE_CREATE_ONLY}, e.BinExistsError),
            (6, 8, "new_mhbin", {"flags": aerospike.HLL_WRITE_UPDATE_ONLY}, e.BinNotFound),
        ],
    )
    def test_neg_hll_init_mh(self, index_bits, mh_bits, bin, policy, expected_result):
        """
        Invoke hll_init_mh() expecting failures.
        """
        ops = [hll_operations.hll_init(bin, index_bits, mh_bits, policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[0], ops)

    def test_pos_hll_init(self):
        """
        Invoke hll_init() creating a new HLL.
        """
        ops = [hll_operations.hll_init("new_hll", 6)]

        _, _, _ = self.as_connection.operate(self.test_keys[0], ops)

        record = self.as_connection.get(self.test_keys[0])
        assert record[2]["new_hll"]

    @pytest.mark.parametrize(
        "index_bits, bin, policy, expected_result",
        [
            (3, "new_hllbin", None, e.InvalidRequest),
            (20, "new_hllbin", None, e.InvalidRequest),
            (6, "hll_bin", {"flags": aerospike.HLL_WRITE_CREATE_ONLY}, e.BinExistsError),
            (6, "new_hllbin", {"flags": aerospike.HLL_WRITE_UPDATE_ONLY}, e.BinNotFound),
        ],
    )
    def test_neg_hll_init(self, index_bits, bin, policy, expected_result):
        """
        Invoke hll_init() expecting failures.
        """
        ops = [hll_operations.hll_init(bin, index_bits, policy=policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[0], ops)

    def test_neg_hll_refresh_count(self):
        """
        Invoke hll_refresh_count() expecting failures.
        """
        ops = [hll_operations.hll_refresh_count("new_hll")]

        with pytest.raises(e.BinNotFound):
            self.as_connection.operate(self.test_keys[0], ops)

    def test_pos_hll_refresh_count(self):
        """
        Invoke hll_refresh_count().
        """
        ops = [hll_operations.hll_refresh_count("hll_binl")]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)
        assert res["hll_binl"] == 97

    @pytest.mark.parametrize(
        "bin, policy, expected_result",
        [
            ("hll_bine", {}, e.OpNotApplicable),
            ("hll_binl", {"flags": aerospike.HLL_WRITE_CREATE_ONLY}, e.BinExistsError),
            ("new_hll", {"flags": aerospike.HLL_WRITE_UPDATE_ONLY}, e.BinNotFound),
        ],
    )
    def test_neg_hll_set_union(self, bin, policy, expected_result):
        """
        Invoke hll_set_union() expecting failures.
        """

        hll_bin_values = [br.record[2]["hll_binl"] for br in
                          self.as_connection.batch_read(self.test_keys).batch_records]

        ops = [hll_operations.hll_set_union(bin, hll_bin_values, policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[4], ops)

    @pytest.mark.parametrize(
        "bin, ubin, policy, expected_res",
        [("hll_bin", "hll_bin", {}, 10), ("hll_bine", "hll_binl", {"flags": aerospike.HLL_WRITE_ALLOW_FOLD}, 5)],
    )
    def test_pos_hll_set_union(self, bin, ubin, expected_res, policy):
        """
        Invoke hll_set_union().
        """

        hll_bin_values = [br.record[2]["hll_binu"] for br in
                          self.as_connection.batch_read(self.test_keys).batch_records]

        ops = [hll_operations.hll_set_union(bin, [hll_bin_values[4]], policy)]

        _, _, _ = self.as_connection.operate(self.test_keys[4], ops)

        union_hll = self.as_connection.get(self.test_keys[4])[2][ubin]

        ops = [
            hll_operations.hll_get_intersect_count(bin, [union_hll]),
            hll_operations.hll_get_intersect_count("hll_binu", [union_hll]),
        ]

        _, _, res = self.as_connection.operate(self.test_keys[4], ops)

        assert res[bin] == expected_res
        assert res["hll_binu"] == 5

    @pytest.mark.parametrize(
        "bin, policy, expected_result", [("new_hll", {"flags": aerospike.HLL_WRITE_CREATE_ONLY}, e.OpNotApplicable)]
    )
    def test_neg_hll_update(self, bin, policy, expected_result):
        """
        Invoke hll_update() with errors.
        """
        ops = [hll_operations.hll_add(bin, ["key1", "key2", "key3"], policy=policy)]

        with pytest.raises(expected_result):
            self.as_connection.operate(self.test_keys[0], ops, policy=policy)

    def test_pos_hll_update(self):
        """
        Invoke hll_update() creating a new HLL.
        """
        ops = [hll_operations.hll_add("hll_bine", ["key1", "key2", "key3"])]

        _, _, res = self.as_connection.operate(self.test_keys[0], ops)

        assert res["hll_bine"] == 3

    def test_get_put_operate_hll(self):
        """
        Can you read and write HLL bins to the server and still perform HLL operations on those bins?
        """
        _, _, rec = self.as_connection.get(self.test_keys[0])
        assert type(rec["mh_bin"]) == HyperLogLog

        self.as_connection.put(self.test_keys[0], {"mh_bin": rec["mh_bin"]})

        # mh_bin should return the same results as before reading and rewritting the bin
        ops = [hll_operations.hll_describe("mh_bin")]
        _, _, res = self.as_connection.operate(self.test_keys[0], ops)
        assert res["mh_bin"] == [6, 12]

    def test_put_get_hll_list(self):
        """
        This is to cover putting nested HLLs in the server
        Since the conversion for nested HLLs to the C client equivalent is separate from top-level HLLs
        """
        # Test setup to retrieve an HLL bin
        _, _, rec = self.as_connection.get(self.test_keys[0])

        self.as_connection.put(
            self.test_keys[0],
            {
                "hll_list": [
                    rec["hll_bin"]
                ]
            }
        )
        # Verify we stored the HLL in the list as an HLL type
        _, _, rec = self.as_connection.get(self.test_keys[0])
        assert type(rec["hll_list"][0]) == HyperLogLog

    def test_hll_superclass(self):
        assert issubclass(HyperLogLog, bytes)

    def test_hll_str_repr(self):
        bytes_obj = b'asdf'
        hll = HyperLogLog(bytes_obj)

        expected_repr = f"{hll.__class__.__name__}({bytes_obj.__repr__()})"
        assert str(hll) == expected_repr
        assert repr(hll) == expected_repr

        hll_from_eval = eval(expected_repr)
        # We compare HLL instances by comparing their bytes values
        assert hll == hll_from_eval

        # Negative test for comparing HLL values
        different_hll = HyperLogLog(b'asdff')
        assert different_hll != hll_from_eval
