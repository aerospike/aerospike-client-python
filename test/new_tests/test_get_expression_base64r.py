# -*- coding: utf-8 -*-

import pytest

from aerospike import exception as e
from .test_base_class import TestBaseClass
from aerospike_helpers.expressions import (
    And,
    Eq,
    IntBin,
    ListGetByIndex,
    ListGetByIndexRange,
    ListGetByIndexRangeToEnd,
    ListGetByRank,
    ListGetByRankRange,
    ListGetByRankRangeToEnd,
    ListGetByValue,
    ListGetByValueList,
    ListGetByValueRange,
    ListGetByValueRelRankRange,
    ListGetByValueRelRankRangeToEnd,
    ListSize,
    ResultType,
)

import aerospike
from . import base64_helpers


@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
class TestGetExpressionBase64(object):
    # Backwards compatibility test
    # Setup client connection before deciding which method to use
    @pytest.fixture(scope="class", params=["client", "aerospike"])
    def get_expression_base64_method(self, request, as_connection, connection_config):
        return base64_helpers.get_expression_base64_method(self, request.param)

    def test_get_expression_base64_pos(self, get_expression_base64_method):

        expr = Eq(IntBin("bin1"), 6).compile()
        b64 = get_expression_base64_method(expr)
        assert b64 == "kwGTUQKkYmluMQY="

    def test_get_expression_base64_large_expression_pos(self, get_expression_base64_method):
        bin = "ilist_bin"
        expr = And(
            Eq(
                ListGetByValueRelRankRange(
                    None,
                    aerospike.LIST_RETURN_COUNT,
                    ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0, bin),
                    1,
                    3,
                    bin,
                ),
                2,
            ),
            Eq(
                ListGetByValue(
                    None,
                    aerospike.LIST_RETURN_INDEX,
                    6,
                    ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, 1, 7, bin),
                ),
                [2],
            ),
            Eq(
                ListGetByValueList(
                    None,
                    aerospike.LIST_RETURN_COUNT,
                    [2, 6],
                    ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 1, bin),
                ),
                2,
            ),
            Eq(
                ListGetByIndexRangeToEnd(
                    None,
                    aerospike.LIST_RETURN_COUNT,
                    1,
                    ListGetByIndexRange(
                        None,
                        aerospike.LIST_RETURN_VALUE,
                        1,
                        3,
                        bin,
                    ),
                ),
                1,
            ),
            Eq(
                ListGetByRank(
                    None,
                    aerospike.LIST_RETURN_RANK,
                    ResultType.INTEGER,
                    1,
                    ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, bin),
                ),
                1,
            ),
            Eq(ListGetByRankRange(None, aerospike.LIST_RETURN_COUNT, 1, ListSize(None, bin), bin), 2),
        ).compile()

        b64 = get_expression_base64_method(expr)
        expected = (
            "lxCTAZV/AgCVGwWVfwIAkxMHAJNRBKlpbGlzdF9iaW4BA5NRBKlpbGlzdF9iaW4CkwGVfwQAkxYBBpV/BAC"
            "UGQcBB5NRBKlpbGlzdF9iaW6SfpECkwGVfwIAkxcFkn6SAgaVfwQAlBsHAQGTUQSpaWxpc3RfYmluApMBlX8CAJMYBQGVfwQAlBgHAQOTU"
            "QSpaWxpc3RfYmluAZMBlX8CAJMVAwGVfwQ"
            "AkxoHAZNRBKlpbGlzdF9iaW4BkwGVfwIAlBoFAZV/AgCREJNRBKlpbGlzdF9iaW6TUQSpaWxpc3RfYmluAg=="
        )

        assert b64 == expected

    def test_get_expression_base64_bad_filter_neg(self, get_expression_base64_method):
        with pytest.raises(e.ParamError):
            get_expression_base64_method("bad_filter")

    def test_get_expression_base64_bad_list_filter_neg(self, get_expression_base64_method):
        with pytest.raises(e.ParamError):
            get_expression_base64_method(["hi"])
