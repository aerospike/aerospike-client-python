# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from .as_status_codes import AerospikeStatus

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.skip(reason="These will crash until pytest version is > 3.0.5")
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

        if TestBaseClass.has_ldt_support():
            key = ('test', u'demo', 'ldt_key')
            self.llist_bin = as_connection.llist(key, 'llist_key')
            self.llist_bin.add(10)
            self.record_count += 1

        def teardown():
            for i in range(19):
                key = ('test', u'demo', i)
                as_connection.remove(key)

            key = ('test', 'demo', 122)
            as_connection.remove(key)

            if TestBaseClass.has_ldt_support():
                self.llist_bin.remove(10)
                key = ('test', 'demo', 'ldt_key')
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_scan_with_existent_ns_and_set(self):

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback)

        assert len(records) == self.record_count

    def test_scan_with_existent_ns_and_none_set(self):

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, None)

        scan_obj.foreach(callback)

        assert len(records) == self.record_count

    def test_scan_with_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 2000})

        assert len(records) == self.record_count

    def test_scan_with_socket_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'socket_timeout': 9876})

        assert len(records) == self.record_count

    def test_scan_with_callback_returning_false(self):
        """
            Invoke scan() with callback function returns false
        """

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            if len(records) == 10:
                return False
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 1000})
        assert len(records) == 10

    def test_scan_with_unicode_set(self):
        records = []
        st = u'demo'

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, st)

        scan_obj.foreach(callback)
        assert len(records) == self.record_count

    def test_scan_with_select_clause(self):

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.select('name')

        scan_obj.foreach(callback)
        # Only 19/20 records contain a bin called 'name'
        assert 19 <= len(records) < self.record_count

    def test_scan_with_results_method(self):

        ns = 'test'
        st = 'demo'

        scan_obj = self.as_connection.scan(ns, st)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results()
        # Only 19/20 records contain a bin called 'name' or 'age'
        # Depending on ldt support this could be record count -1 or minus 2
        assert 19 <= len(records) < self.record_count

    def test_scan_with_options_positive(self):
        """
            Invoke scan() with options positive
        """

        records = []

        options = {
            "percent": 100,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {}, options)

        assert len(records) == self.record_count

    @pytest.mark.xfail(reason="Server does not respect percent < 100")
    def test_scan_with_options_percent_partial(self):
        """
            Invoke scan() with options negative
        """
        def callback(input_tuple):
            key, _, _ = input_tuple
            records.append(key)

        ns = 'test'
        st = 'demo'
        records = []
        options = {
            "percent": 80,
            "concurrent": True,
            "priority": aerospike.SCAN_PRIORITY_HIGH
        }
        scan_obj = self.as_connection.scan(ns, st)
        scan_obj.foreach(callback, {}, options)
        assert len(records) >= 16 and len(records) < 18

    def test_scan_with_options_nobins_true(self):
        """
            Invoke scan() with nobins
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {"nobins": True}

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert len(records) == self.record_count
        for record in records:
            assert record == {}  # The bins portion should be an empty dict

    def test_scan_with_options_nobins_false(self):
        """
            Invoke scan() with nobins
        """
        ns = 'test'
        st = 'demo'

        records = []

        options = {"nobins": False}

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(ns, st)

        scan_obj.foreach(callback, {}, options)

        assert len(records) == self.record_count

        for record in records:
            assert record != {}

    def test_scan_with_options_includeldt_positive(self):
        """
            Invoke scan() with include ldt set to True
        """
        if not TestBaseClass.has_ldt_support():
            pytest.skip()

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

        scan_obj = self.as_connection.scan(ns, st)

        scan_obj.foreach(callback, {}, options)
        value = 0
        for x in records:
            if 'llist_key' in x.keys():
                value = x['llist_key']

        assert value == [10]
        assert len(records) != 0

    def test_scan_with_options_includeldt_negative(self):
        """
            Invoke scan() with include ldt set to False
        """
        if not TestBaseClass.has_ldt_support():
            pytest.skip()
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

        scan_obj = self.as_connection.scan(ns, st)

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
        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback)

        assert len(records) == self.record_count

        records = []
        scan_obj.foreach(callback)

        assert len(records) == self.record_count

    def test_scan_with_multiple_results_call_on_same_scan_object(self):

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.select(u'name', u'age')

        records = scan_obj.results()
        assert 19 <= len(records) < self.record_count

        records = []
        records = scan_obj.results()
        assert 19 <= len(records) < self.record_count

    def test_scan_with_select_binnames_bytearray(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.select(self.bytearray_bin)

        scan_obj.foreach(callback)

        assert len(records) == 1

    @pytest.mark.skip(
         reason="this raises a SystemError, and crashes pytest")
    def test_scan_without_any_parameter(self):

        with pytest.raises(e.ParamError) as err:
            pytest.set_trace()
            scan_obj = self.as_connection.scan()
            assert True

    def test_scan_with_non_existent_ns_and_set(self):

        ns = 'namespace'
        st = 'set'

        records = []
        scan_obj = self.as_connection.scan(ns, st)

        def callback(input_tuple):
            _, _, bins = input_tuple
            records.append(bins)

        with pytest.raises(e.NamespaceNotFound) as err_info:
            scan_obj.foreach(callback)
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_scan_with_none_ns_and_set(self):

        ns = None
        st = None

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.scan(ns, st)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_scan_with_select_bin_integer(self):
        """
            Invoke scan() with select bin is of type integer.
        """
        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ParamError) as err_info:
            scan_obj.select(22, 'name')

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_scan_with_callback_contains_error(self):
        records = []

        def callback(input_tuple):
            _, _, bins = input_tuple
            raise Exception("callback error")
            records.append(bins)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {'timeout': 1000})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_with_callback_non_callable(self):
        records = []

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(5)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_with_callback_wrong_number_of_args(self):

        def callback():
            pass

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT
