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


@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestGetExpressionBase64(object):
    @pytest.fixture(autouse=True)
    def setup_method(self, request, as_connection):
        pass

    def test_get_expression_base64_pos(self):
        expr = Eq(IntBin("bin1"), 6).compile()
        b64 = self.as_connection.get_expression_base64(expr)
        assert b64 == "kwGTUQKkYmluMQY="

    def test_get_expression_base64_large_expression_pos(self):
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

        b64 = self.as_connection.get_expression_base64(expr)
        expected = (
            "lxCTAZV/AgCVGwWVfwIAkxMHAJNRBKlpbGlzdF9iaW4BA5NRBKlpbGlzdF9iaW4CkwGVfwQAkxYBBpV/BAC"
            "UGQcBB5NRBKlpbGlzdF9iaW6SfpECkwGVfwIAkxcFkn6SAgaVfwQAlBsHAQGTUQSpaWxpc3RfYmluApMBlX8CAJMYBQGVfwQAlBgHAQOTU"
            "QSpaWxpc3RfYmluAZMBlX8CAJMVAwGVfwQ"
            "AkxoHAZNRBKlpbGlzdF9iaW4BkwGVfwIAlBoFAZV/AgCREJNRBKlpbGlzdF9iaW6TUQSpaWxpc3RfYmluAg=="
        )

        assert b64 == expected

    def test_get_expression_base64_bad_filter_neg(self):
        with pytest.raises(e.ParamError):
            self.as_connection.get_expression_base64("bad_filter")

    def test_get_expression_base64_bad_list_filter_neg(self):
        with pytest.raises(e.ParamError):
            self.as_connection.get_expression_base64(["hi"])
