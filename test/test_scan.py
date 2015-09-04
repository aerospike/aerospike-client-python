# -*- coding: utf-8 -*-

import pytest
import sys
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestScan(TestBaseClass):

    def setup_method(self, method):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)

        for i in xrange(20):
            key = ('test', u'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            self.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardown method
        """

        for i in xrange(20):
            key = ('test', u'demo', i)
            self.client.remove(key)

        self.client.close()

    def test_scan_without_any_parameter(self):

        scan_obj = None
        with pytest.raises(TypeError) as typeError:
            scan_obj = self.client.scan()
            scan_obj.foreach()

        assert "Required argument 'callback' (pos 1) not found" in typeError.value

    def test_scan_with_non_existent_ns_and_set(self):

        ns = 'namespace'
        st = 'set'

        records = []
        scan_obj = None
        scan_obj = self.client.scan(ns, st)

        def callback((key, meta, bins)):
            records.append(bins)

        try:
            scan_obj.foreach(callback)

        except NamespaceNotFound as exception:
            assert exception.code == 20L
        except ServerError as exception:
            assert exception.code == 1L

    def test_scan_with_none_ns_and_set(self):

        ns = None
        st = None

        try:
            scan_obj = self.client.scan( ns, st )

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == 'Parameters are incorrect'

    def test_scan_with_existent_ns_and_set(self):

        ns = 'test'
        st = 'demo'

        records = []
        scan_obj = None

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_existent_ns_and_none_set(self):

        ns = 'test'
        st = 'demo'

        records = []
        scan_obj = None

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, None)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {'timeout': 2000})

        assert len(records) != 0

    def test_scan_with_callback_contains_error(self):
        pytest.xfail("segfault!")
        ns = 'test'
        st = 'demo'

        records = []
        scan_obj = None
        val = 1
        def callback( (key, meta, bins) ):
            val += 1
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        try:
            scan_obj.foreach(callback, { 'timeout' : 1000 })

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == "Callback function contains an error"

    def test_scan_with_callback_returning_false(self):
        """
            Invoke scan() with callback function returns false
        """
        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None

        def callback((key, meta, bins)):
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

        scan_obj = None

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_select_clause(self):

        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.select('name')

        scan_obj.foreach(callback)

        assert len(records) != 0

    def test_scan_with_results_method(self):

        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None
        scan_obj = self.client.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results()

        assert len(records) != 0

    def test_scan_with_select_bin_integer(self):
        """
            Invoke scan() with select bin is of type integer.
        """
        scan_obj = None
        scan_obj = self.client.scan('test', 'demo')

        try:
            scan_obj.select(22, 'test_age')

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == 'Bin name should be of type string'

    def test_scan_with_options_positive(self):
        """
            Invoke scan() with options positive
        """
        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None
        options = {
            "percent": 100,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert len(records) != 0

    def test_scan_with_options_percent_negative(self):
        """
            Invoke scan() with options negative
        """
        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None
        options = {
            "percent": 80,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert records == []

    def test_scan_with_options_nobins(self):
        """
            Invoke scan() with nobins
        """
        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None
        options = {"priority": aerospike.SCAN_PRIORITY_HIGH, "nobins": True}

        def callback((key, meta, bins)):
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

        scan_obj = None
        options = {"priority": aerospike.SCAN_PRIORITY_HIGH, "nobins": "true"}

        def callback((key, meta, bins)):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        try:
            scan_obj.foreach(callback, { 'timeout' : 1000 }, options)

        except ParamError as exception:
            assert exception.code == -2L
            assert exception.msg == 'Invalid value(type) for nobins'

    def test_scan_with_multiple_foreach_on_same_scan_object(self):
        """
            Invoke multiple foreach on same scan object.
        """
        ns = 'test'
        st = 'demo'

        records = []

        scan_obj = None

        def callback((key, meta, bins)):
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

        scan_obj = None
        scan_obj = self.client.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = []
        records = scan_obj.results()
        assert len(records) != 0

        records = []
        records = scan_obj.results()
        assert len(records) != 0
