# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from aerospike import predexp as as_predexp
from .as_status_codes import AerospikeStatus

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestScan(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        for i in range(19):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)

        key = ('test', u'demo', 122)
        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": bytearray("asd;adk\0kj", "utf-8"),
                  "val": u"john"}]
        # Creates a record with the key 122, with one bytearray key.
        self.bytearray_bin = bytearray("asd;adk\0kj", "utf-8")
        as_connection.operate(key, llist)
        self.record_count = 20

        def teardown():
            for i in range(19):
                key = ('test', u'demo', i)
                as_connection.remove(key)

            key = ('test', 'demo', 122)
            as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_scan_with_predexp_policy(self):

        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        ns = 'test'
        st = 'demo'

        records = []

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value('name4'),
            as_predexp.string_equal(),
            as_predexp.predexp_not()
        ]

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 2000, 'predexp': predexp})

        assert len(records) == self.record_count - 1

    def test_scan_with_predexp_policy_no_set(self):

        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        ns = 'test'
        st = None

        records = []

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value('name4'),
            as_predexp.string_equal(),
            as_predexp.predexp_not()
        ]

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 2000, 'predexp': predexp})

        assert len(records) == self.record_count - 1

    def test_scan_with_results_method_and_predexp(self):

        from .test_base_class import TestBaseClass
        if TestBaseClass.major_ver >= 5 and TestBaseClass.minor_ver >=7:
            # print("TestBaseClass.major_ver:", TestBaseClass.major_ver, "TestBaseClass.minor_ver:", TestBaseClass.minor_ver)
            pytest.skip(
                'It deprecated and it only applies to < 5.7 earlier and enterprise edition')

        ns = 'test'
        st = 'demo'

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value('name4'),
            as_predexp.string_equal(),
            as_predexp.predexp_not()
        ]

        scan_obj = self.as_connection.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results({'predexp': predexp})
        # Only 19/20 records contain a bin called 'name' or 'age'
        # Depending on ldt support this could be record count -2 or minus 3
        assert 18 <= len(records) < self.record_count - 1

    def test_scan_with_invalid_predexp_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        predexp = [
            as_predexp.string_bin('name'),
            as_predexp.string_value(2),
            as_predexp.string_equal(),
            as_predexp.predexp_not()
        ]

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ParamError):
            scan_obj.foreach(callback, {'timeout': 2000, 'predexp': predexp})
