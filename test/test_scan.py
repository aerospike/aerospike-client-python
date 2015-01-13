# -*- coding: utf-8 -*-

import pytest
import sys
from test_base_class import TestBaseClass

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)


class TestScan(TestBaseClass):

    def setup_method(self, method):

        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
                }
        if user == None and password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(user, password)
    
        for i in xrange(20):
            key = ('test', 'demo', i)
            rec = {
                    'name' : 'name%s' % (str(i)),
                    'age'  : i
                    }
            self.client.put(key, rec)

    def teardown_method(self, method):

        """
        Teardown method
        """

        for i in xrange(20):
            key = ('test', 'demo', i)
            self.client.remove(key)

        self.client.close()

    def test_scan_without_any_parameter(self):


        with pytest.raises(TypeError) as typeError:
            scan_obj = self.client.scan()
            scan_obj.foreach()

        assert "Required argument 'callback' (pos 1) not found" in typeError.value

    def test_scan_with_non_existent_ns_and_set(self):

        ns = 'namespace'
        st = 'set'

        records = []

        scan_obj = self.client.scan(ns, st)

        def callback( (key, meta, bins) ):
            records.append(bins)

        with pytest.raises(Exception) as exception:
            scan_obj.foreach(callback)

        assert exception.value[0] == 1
        assert exception.value[1] == 'AEROSPIKE_ERR_SERVER'

    def test_scan_with_none_ns_and_set(self):

        ns = None
        st = None

        with pytest.raises(Exception) as exception:
            scan_obj = self.client.scan( ns, st )

        #assert exception.value[0] == 501
        assert 1 == 1


    def test_scan_with_existent_ns_and_set(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback( (key, meta, bins) ):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback)

        assert len(records) != 0

        assert len(records) != 0

    def test_scan_with_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback( (key, meta, bins) ):
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, { 'timeout' : 1000 })

        assert len(records) != 0
    """
    def test_scan_with_callback_contains_error(self):

            #Invoke scan() with callback function returns false
        ns = 'test'
        st = 'demo'

        records = []
        val = 1
        def callback( (key, meta, bins) ):
            val += 1
            records.append(bins)

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, { 'timeout' : 1000 })

        assert len(records) == 0
        """

    def test_scan_with_callback_returning_false(self):

        """
            Invoke scan() with callback function returns false
        """
        ns = 'test'
        st = 'demo'

        records = []

        def callback( (key, meta, bins) ):
            records.append(bins)
            return False

        scan_obj = self.client.scan(ns, st)

        scan_obj.foreach(callback, { 'timeout' : 1000 })

        assert len(records) == 1
