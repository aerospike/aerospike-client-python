# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus
from aerospike_helpers import cdt_ctx
from aerospike_helpers.expressions import *
from aerospike_helpers.operations import map_operations
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import hll_operations
from aerospike_helpers.operations import expression_operations as expressions
from aerospike_helpers.operations import operations
from math import sqrt, ceil, floor

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# Constants
_NUM_RECORDS = 9


def verify_multiple_expression_result(client, test_ns, test_set, expr, op_bin, expected):
    keys = [(test_ns, test_set, i) for i in range(_NUM_RECORDS + 1)]

    # batch get
    res = [rec for rec in client.get_many(keys, policy={'expressions': expr}) if rec[2]]

    assert len(res) == expected


class TestUsrDefinedClass():

    __test__ = False

    def __init__(self, i):
        self.data = i


class TestExpressions(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        HLL_ops = [
            hll_operations.hll_add('hll_bin', ['key%s' % str(i) for i in range(10000)], 15, 49),
            hll_operations.hll_add('hll_bin2', ['key%s' % str(i) for i in range(5000, 15000)], 15, 49),
            hll_operations.hll_add('hll_bin3', ['key%s' % str(i) for i in range(20000, 30000)], 15, 49)
        ]

        for i in range(_NUM_RECORDS):
            _, _, _ = self.as_connection.operate((self.test_ns, self.test_set, i), HLL_ops)

        def teardown():
            for i in range(_NUM_RECORDS):
                key = (self.test_ns, self.test_set, i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def relative_count_error(self, n_index_bits, expected):
        return (expected * (1.04 / sqrt(2 ** n_index_bits)) * 8)

    def relative_intersect_error(self, n_index_bits, bin_counts, bin_intersect_count):
        sigma = (1.04 / sqrt(2 ** n_index_bits))
        rel_err_sum  = 0
        for count in bin_counts:
            rel_err_sum += ((sigma * count) ** 2)
        rel_err_sum += (sigma * (bin_intersect_count ** 2))

        return sqrt(rel_err_sum)

    @pytest.mark.parametrize("policy, listp, index_bc, mh_bc, bin, expected", [ 
        (None, ['key%s' % str(i) for i in range(11000, 16000)], 15, None, 'hll_bin', 15000),
        (None, ['key%s' % str(i) for i in range(11000, 16000)], None, None, 'hll_bin', 15000),
        (None, ['key%s' % str(i) for i in range(11000, 16000)], 15, 49, 'hll_bin', 15000),
        ({'flags': aerospike.HLL_WRITE_NO_FAIL}, ['key%s' % str(i) for i in range(11000, 16000)], None, None, 'hll_bin', 15000)
    ])
    def test_hll_add_pos(self, policy, listp, index_bc, mh_bc, bin, expected):
        """
        Test the HLLAdd expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(15, expected))
        lower_lim = floor(expected - self.relative_count_error(15, expected))
        expr = And(
                GE(
                    HLLGetCount(
                        HLLAdd(policy, listp, index_bc, mh_bc, bin)),
                    lower_lim
                ),
                LE(
                    HLLGetCount(
                        HLLAdd(policy, listp, index_bc, mh_bc, bin)),
                    upper_lim
                ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("policy, index_bc, mh_bc, bin, expected", [
        (None, 12, None, 'hll_bin', {"": [12, 49]}),
        (None, None, None, 'hll_bin', {"": [15, 49]}),
        (None, 8, 20, 'hll_bin', {"": [8, 20]}),
        ({'flags': aerospike.HLL_WRITE_CREATE_ONLY | aerospike.HLL_WRITE_NO_FAIL}, 15, 49, 'hll_bin', {"": [15, 49]})
    ])
    def test_hll_init_pos(self, policy, index_bc, mh_bc, bin, expected):
        """
        Test the HLLInit expression.
        """

        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = HLLDescribe(HLLInit(policy, index_bc, mh_bc, bin))

        ops = [
            expressions.expression_read("", expr.compile())
        ]

        _, _, res = self.as_connection.operate((self.test_ns, self.test_set, 0), ops)
        assert res == expected

    @pytest.mark.parametrize("policy, index_bc, mh_bc, bin, expected", [
        # OpNotApplicable because read tries to read failed expression
        ({'flags': aerospike.HLL_WRITE_CREATE_ONLY}, 8, 20, 'hll_bin', e.OpNotApplicable)
    ])
    def test_hll_init_neg(self, policy, index_bc, mh_bc, bin, expected):
        """
        Test the HLLInit expression expecting failure.
        """

        if self.server_version < [5, 6]:
            pytest.mark.xfail(reason="Servers older than 5.6 do not support 6.0.0 expressions")
            pytest.xfail()

        expr = HLLDescribe(HLLInit(policy, index_bc, mh_bc, bin))

        ops = [
            expressions.expression_read(bin, expr.compile())
        ]

        with pytest.raises(expected):
            self.as_connection.operate((self.test_ns, self.test_set, 0), ops)

    @pytest.mark.parametrize("bin, expected, hll_bins", [
        ('hll_bin', 25000, ['hll_bin', 'hll_bin2', 'hll_bin3']),
        ('hll_bin', 20000, ['hll_bin3']),
    ])
    def test_hll_get_union_pos(self, bin, expected, hll_bins):
        """
        Test the HLLGetUnion expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(10, expected))
        lower_lim = floor(expected - self.relative_count_error(10, expected))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2][hll_bin] for hll_bin in hll_bins] if len(hll_bins) > 1 else record[2][hll_bins[0]]
        expr = And(
                    GE(
                        HLLGetCount(
                            HLLGetUnion(records, bin)),
                        lower_lim
                    ),
                    LE(
                        HLLGetCount(
                            HLLGetUnion(records, bin)),
                        upper_lim
                    ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 25000)
    ])
    def test_hll_get_union_count_pos(self, bin, expected):
        """
        Test the HLLGetUnionCount expression.
        """

        upper_lim = ceil(expected + self.relative_count_error(10, expected))
        lower_lim = floor(expected - self.relative_count_error(10, expected))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin'], record[2]['hll_bin2'], record[2]['hll_bin3']]
        expr = And(
                    GT(
                        HLLGetUnionCount(records, bin),
                        lower_lim
                    ),
                    LE(
                        HLLGetUnionCount(records, bin),
                        upper_lim
                    ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 5000)
    ])
    def test_hll_get_intersect_count_pos(self, bin, expected):
        """
        Test the HLLGetIntersectCount expression.
        """

        upper_lim = ceil(expected + self.relative_intersect_error(10, [10000, 10000], 5000))
        lower_lim = floor(expected - self.relative_intersect_error(10, [10000, 10000], 5000))
        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin2']]
        expr = And(
                    GE(
                        HLLGetIntersectCount(records, bin),
                        lower_lim
                    ),
                    LE(
                        HLLGetIntersectCount(records, bin),
                        upper_lim
                    ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', 0.33)
    ])
    def test_hll_get_similarity_pos(self, bin, expected):
        """
        Test the HLLGetSimilarity expression.
        """

        record = self.as_connection.get(('test', u'demo', 0))
        records = [record[2]['hll_bin2']]
        expr = And(
                    GE(
                        HLLGetSimilarity(records, bin),
                        expected - 0.03
                    ),
                    LE(
                        HLLGetSimilarity(records, bin),
                        expected + 0.03
                    ),
        )

        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin, expected", [
        ('hll_bin', [15, 49])
    ])
    def test_hll_describe_pos(self, bin, expected):
        """
        Test the HLLDescribe expression.
        """

        expr = Eq(HLLDescribe(bin), expected)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)

    @pytest.mark.parametrize("bin", [
        ('hll_bin')
    ])
    def test_hll_may_contain_pos(self, bin):
        """
        Test the HLLMayContain expression.
        """

        expr = Eq(HLLMayContain(["key1", "key2", "key3"], HLLBin(bin)), 1)
        verify_multiple_expression_result(self.as_connection, self.test_ns, self.test_set, expr.compile(), bin, _NUM_RECORDS)