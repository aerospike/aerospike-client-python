# -*- coding: utf-8 -*-

import pytest
import sys

from aerospike import exception as e
from .test_base_class import TestBaseClass
from aerospike_helpers.expressions import *

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestSetXDRFilter(object):

    @pytest.fixture(autouse=True)
    def setup_method(self, request, as_connection):
        try:
            dc_request = "get-config:context=xdr"
            addr_name_dict = {}
            nodes = self.as_connection.get_node_names()
            node_name = nodes[0]["node_name"]

            dc_response = self.as_connection.info_single_node(dc_request, node_name)
            self.dc = dc_response.split(",")[0].split("=")[2]
            ns_request = "get-config:context=xdr;dc=%s" % self.dc
            ns_response = self.as_connection.info_single_node(ns_request, node_name)
            self.ns = ns_response.split("namespaces=")[1].split(";")[0]
        except Exception as exc:
            pytest.skip("Could not parse a data center or namespace, xdr may be disabled, skipping set_xdr_flags.")

    def test_set_xdr_filter_pos(self):
        response = self.as_connection.set_xdr_filter(self.dc, self.ns, (Eq(IntBin("bin1"), 6).compile()))
        assert response == "xdr-set-filter:dc=%s;namespace=%s;exp=kwGTUQKkYmluMQY=\tok\n" % (self.dc, self.ns)

    def test_set_xdr_filter_large_expression_pos(self):
        bin = "ilist_bin"
        expr = And(
            Eq(
                ListGetByValueRelRankRange(None, aerospike.LIST_RETURN_COUNT, 
                    ListGetByIndex(None, aerospike.LIST_RETURN_VALUE, ResultType.INTEGER, 0, bin), 1, 3, bin),
                2),
            Eq(
                ListGetByValue(None, aerospike.LIST_RETURN_INDEX, 6,
                    ListGetByValueRange(None, aerospike.LIST_RETURN_VALUE, 1, 7, bin)),
                [2]),
            Eq(
                ListGetByValueList(None, aerospike.LIST_RETURN_COUNT, [2, 6], 
                    ListGetByValueRelRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, 1, bin)),
                2),
            Eq(
                ListGetByIndexRangeToEnd(None, aerospike.LIST_RETURN_COUNT, 1,
                    ListGetByIndexRange(None, aerospike.LIST_RETURN_VALUE, 1, 3, bin,)),
                1),
            Eq(
                ListGetByRank(None, aerospike.LIST_RETURN_RANK, ResultType.INTEGER, 1,
                    ListGetByRankRangeToEnd(None, aerospike.LIST_RETURN_VALUE, 1, bin)),
                1),
            Eq(
                ListGetByRankRange(None, aerospike.LIST_RETURN_COUNT, 1, ListSize(None, bin), bin),
                2
            )
        ).compile()

        response = self.as_connection.set_xdr_filter(self.dc, self.ns, expr)
        expected = ("xdr-set-filter:dc=%s;namespace=%s;exp=lxCTAZV/AgCVGwWVfwIAkxMHAJNRBKlpbGlzdF9iaW4BA5NRBKlpbGlzdF9iaW4CkwGVfwQAkxYBBpV/BAC"
                    "UGQcBB5NRBKlpbGlzdF9iaW6SfpECkwGVfwIAkxcFkn6SAgaVfwQAlBsHAQGTUQSpaWxpc3RfYmluApMBlX8CAJMYBQGVfwQAlBgHAQOTUQSpaWxpc3RfYmluAZMBlX8CAJMVAwGVfwQ"
                    "AkxoHAZNRBKlpbGlzdF9iaW4BkwGVfwIAlBoFAZV/AgCREJNRBKlpbGlzdF9iaW6TUQSpaWxpc3RfYmluAg==\tok\n"
                    ) % (self.dc, self.ns)
        assert response == expected

    def test_set_xdr_filter_none_filter_pos(self):
        response = self.as_connection.set_xdr_filter(self.dc, self.ns, None)
        assert response == "xdr-set-filter:dc=%s;namespace=%s;exp=null\tok\n" % (self.dc, self.ns)

    def test_set_xdr_filter_bad_filter_neg(self):
        with pytest.raises(e.ParamError):
            self.as_connection.set_xdr_filter(self.dc, self.ns, "bad_filter")
