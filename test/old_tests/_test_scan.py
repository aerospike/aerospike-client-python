# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestScan(TestBaseClass):

    def setup_method(self, method):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        for i in range(19):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            self.client.put(key, rec)

        key = ('test', u'demo', 122)
        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": bytearray("asd;adk\0kj", "utf-8"),
                  "val": u"john"}]
        self.client.operate(key, llist)

        if TestBaseClass.has_ldt_support():
            key = ('test', u'demo', 'ldt_key')
            self.llist_bin = self.client.llist(key, 'llist_key')
            self.llist_bin.add(10)

    def teardown_method(self, method):
        """
        Teardown method
        """

        for i in range(19):
            key = ('test', u'demo', i)
            self.client.remove(key)

        key = ('test', 'demo', 122)
        self.client.remove(key)

        if TestBaseClass.has_ldt_support():
            self.llist_bin.remove(10)
            key = ('test', 'demo', 'ldt_key')
            self.client.remove(key)

        self.client.close()

    def test_scan_without_any_parameter(self):

        try:
            scan_obj = self.client.scan()
            scan_obj.foreach()

        except Exception:
            pass

    def test_scan_with_non_existent_ns_and_set(self):

        ns = 'namespace'
        st = 'set'

        records = []
        scan_obj = self.client.scan(ns, st)

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        try:
            scan_obj.foreach(callback)

        except e.NamespaceNotFound as exception:
            assert exception.code == 20
        except e.ServerError as exception:
            assert exception.code == 1

    def test_scan_with_none_ns_and_set(self):

        ns = None
        st = None

        try:
            self.client.scan(ns, st)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Parameters are incorrect'

    def test_scan_with_existent_ns_and_set(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_existent_ns_and_none_set(self):

        ns = 'test'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, None)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {'timeout': 2000})

        assert len(records) != 0

    def test_scan_with_callback_contains_error(self):
        ns = 'test'
        st = 'demo'

        records = []
        val = 1

        def callback(input_tuple):
            _, _, bins = input_tuple
            val += 1
            records.append(bins)

        scan_obj = self.client.scan(ns, st)
        try:
            scan_obj.foreach(callback, {'timeout': 1000})
        except e.ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Callback function raised an exception"

    def test_scan_with_callback_returning_false(self):
        """
            Invoke scan() with callback function returns false
        """
        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            if len(records) == 10:
                return False
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {'timeout': 1000})
        assert len(records) == 10

    def test_scan_with_unicode_set(self):

        ns = 'test'

        st = u'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_select_clause(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.select('name')

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_results_method(self):

        ns = 'test'
        st = 'demo'

        scan_obj = self.client.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results()

        assert len(records) != 0

    def test_scan_with_select_bin_integer(self):
        """
            Invoke scan() with select bin is of type integer.
        """
        scan_obj = self.client.scan('test', 'demo')

        try:
            scan_obj.select(22, 'test_age')

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Bin name should be of type string'

    def test_scan_with_options_positive(self):
        """
            Invoke scan() with options positive
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {
            "percent": 100,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert len(records) != 0

    @pytest.mark.xfail(reason="Server does not respect percent < 100")
    def test_scan_with_options_percent_partial(self):
        """
            Invoke scan() with options negative
        """
        def callback(input_tuple):
            key, _, _ = input_tuple
            print (key)
            records.append(key)

        ns = 'test'
        st = 'demo'
        records = []
        options = {
            "percent": 80,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        scan_obj = self.client.scan(ns, st)
        scan_obj.foreach(callback, {}, options)
        assert len(records) >= 16 and len(records) < 18

    def test_scan_with_options_nobins(self):
        """
            Invoke scan() with nobins
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {"priority": aerospike.SCAN_PRIORITY_HIGH, "nobins": True}

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert len(records) != 0

    def test_scan_with_options_nobins_false(self):
        """
            Invoke scan() with nobins
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {"priority": aerospike.SCAN_PRIORITY_HIGH, "nobins": "true"}

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        try:
            scan_obj.foreach(callback, {'timeout': 1000}, options)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Invalid value(type) for nobins'

    @pytest.mark.skipif(TestBaseClass.has_ldt_support() == False,
                        reason="LDTs are not enabled for namespace 'test'")
    def test_scan_with_options_includeldt_positive(self):
        """
            Invoke scan() with include ldt set to True
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {
            "percent": 100,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH,
            "include_ldt": True
        }

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)
        value = 0
        for x in records:
            if 'llist_key' in x.keys():
                value = x['llist_key']

        assert value == [10]
        assert len(records) != 0

    @pytest.mark.skipif(TestBaseClass.has_ldt_support() == False,
                        reason="LDTs are not enabled for namespace 'test'")
    def test_scan_with_options_includeldt_negative(self):
        """
            Invoke scan() with include ldt set to False
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {
            "percent": 100,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH,
            "include_ldt": False
        }

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)
        value = 0
        for x in records:
            if 'llist_key' in x.keys():
                value = x['llist_key']

        assert value is None
        assert len(records) != 0

    def test_scan_with_multiple_foreach_on_same_scan_object(self):
        """
            Invoke multiple foreach on same scan object.
        """
        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

        records = []
        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_multiple_results_call_on_same_scan_object(self):

        ns = 'test'
        st = 'demo'

        scan_obj = self.client.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results()
        assert len(records) != 0

        records = []
        records = scan_obj.results()
        assert len(records) != 0

    def test_scan_with_select_binnames_bytearray(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.select(bytearray("asd;adk\0kj", "utf-8"))

        scan_obj.foreach(callback)

        assert len(records) == 1
