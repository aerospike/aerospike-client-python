# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from aerospike_helpers import expressions as exp
from .as_status_codes import AerospikeStatus

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestScanPartition(TestBaseClass):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        self.test_ns = 'test'
        self.test_set = 'demo'

        self.partition_1000_count = 0
        self.partition_1001_count = 0
        self.partition_1002_count = 0
        self.partition_1003_count = 0
        
        as_connection.truncate(self.test_ns, None, 0)

        for i in range(1, 100000):
            put = 0
            key = (self.test_ns, self.test_set, str(i))
            rec_partition = as_connection.get_key_partition_id(self.test_ns, self.test_set, str(i))

            if rec_partition == 1000:
                self.partition_1000_count += 1
                put = 1 
            if rec_partition == 1001:
                self.partition_1001_count += 1
                put = 1 
            if rec_partition == 1002:
                self.partition_1002_count += 1
                put = 1 
            if rec_partition == 1003:
                self.partition_1003_count += 1
                put = 1 
            if put:
                rec = {
                    'i': i,
                    's': 'xyz',
                    'l': [2, 4, 8, 16, 32, None, 128, 256],
                    'm': {'partition': rec_partition, 'b': 4, 'c': 8, 'd': 16}
                }
                as_connection.put(key, rec)
        # print(f"{self.partition_1000_count} records are put in partition 1000, \
        #         {self.partition_1001_count} records are put in partition 1001, \
        #         {self.partition_1002_count} records are put in partition 1002, \
        #         {self.partition_1003_count} records are put in partition 1003")

        def teardown():
            for i in range(1, 100000):
                put = 0
                key = ('test', u'demo', str(i))
                rec_partition = as_connection.get_key_partition_id(self.test_ns, self.test_set, str(i))

                if rec_partition == 1000:
                    self.partition_1000_count += 1
                    put = 1 
                if rec_partition == 1001:
                    self.partition_1001_count += 1
                    put = 1 
                if rec_partition == 1002:
                    self.partition_1002_count += 1
                    put = 1 
                if rec_partition == 1003:
                    self.partition_1003_count += 1
                    put = 1 
                if put:
                   as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_scan_partition_with_existent_ns_and_set(self):

        records = []
        partition_filter = {'begin': 1000, 'count': 1}
        policy = {'max_retries': 100,
                        'max_records': 1000,
                        'partition_filter': partition_filter,
                        'records_per_second': 4000}
        def callback(part_id,input_tuple):
            (_, _, record) = input_tuple
            records.append(record)
            # print(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, policy)

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_existent_ns_and_none_set(self):

        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, None)

        scan_obj.foreach(callback, {'partition_filter': {'begin': 1000, 'count': 1}})

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 1001, 'partition_filter': {'begin': 1000, 'count': 1}})

        assert len(records) == self.partition_1000_count

    # NOTE: This could fail if node record counts are small and unbalanced across nodes.
    @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_scan_partition_with_max_records_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        max_records = self.partition_1000_count // 2

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'max_records': max_records, 'partition_filter': {'begin': 1000, 'count': 1}})
        assert len(records) == self.partition_1000_count // 2

    @pytest.mark.xfail(reason="Might fail depending on record count and distribution.")
    def test_scan_partition_with_all_records_policy(self):
    
        ns = 'test'
        st = 'demo'

        records = []

        max_records = self.partition_1000_count + \
                        self.partition_1001_count + \
                        self.partition_1002_count + \
                        self.partition_1003_count

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'max_records': max_records, 'partition_filter': {'begin': 1000, 'count': 4}})
        assert len(records) == max_records

    def test_scan_partition_with_socket_timeout_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'socket_timeout': 9876, 'partition_filter': {'begin': 1000, 'count': 1}})

        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_records_per_second_policy(self):

        ns = 'test'
        st = 'demo'

        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'records_per_second': 10, 'partition_filter': {'begin': 1000, 'count': 1}})
        assert len(records) == self.partition_1000_count

    def test_scan_partition_with_callback_returning_false(self):
        """
            Invoke scan() with callback function returns false
        """

        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            if len(records) == 10:
                return False
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'timeout': 1000, 'partition_filter': {'begin': 1000, 'count': 1}})
        assert len(records) == 10

    def test_scan_partition_with_results_method(self):

        ns = 'test'
        st = 'demo'

        scan_obj = self.as_connection.scan(ns, st)

        records = scan_obj.results({'partition_filter': {'begin': 1001, 'count': 1}})
        assert len(records) == self.partition_1001_count

    def test_scan_partition_with_multiple_foreach_on_same_scan_object(self):
        """
            Invoke multiple foreach on same scan object.
        """
        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        scan_obj.foreach(callback, {'partition_filter': {'begin': 1001, 'count': 1}})

        assert len(records) == self.partition_1001_count

        records = []
        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)
        scan_obj2.foreach(callback, {'partition_filter': {'begin': 1001, 'count': 1}})

        assert len(records) == self.partition_1001_count

    def test_scan_partition_with_multiple_results_call_on_same_scan_object(self):

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        records = scan_obj.results({'partition_filter': {'begin': 1002, 'count': 1}})
        assert len(records) == self.partition_1002_count

        scan_obj2 = self.as_connection.scan(self.test_ns, self.test_set)
        records = []
        records = scan_obj2.results({'partition_filter': {'begin': 1002, 'count': 1}})
        assert len(records) == self.partition_1002_count

    def test_scan_partition_without_any_parameter(self):

        with pytest.raises(e.ParamError) as err:
            scan_obj = self.as_connection.scan()
            assert True

    def test_scan_partition_with_non_existent_ns_and_set(self):

        ns = 'namespace'
        st = 'set'

        records = []
        scan_obj = self.as_connection.scan(ns, st)

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            records.append(record)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {'partition_filter': {'begin': 1001, 'count': 1}})
        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_partition_with_callback_contains_error(self):
        records = []

        def callback(part_id,input_tuple):
            _, _, record = input_tuple
            raise Exception("callback error")
            records.append(record)

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {'timeout': 1000, 'partition_filter': {'begin': 1001, 'count': 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_partition_with_callback_non_callable(self):
        records = []

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(5, {'partition_filter': {'begin': 1001, 'count': 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_scan_partition_with_callback_wrong_number_of_args(self):

        def callback():
            pass

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        with pytest.raises(e.ClientError) as err_info:
            scan_obj.foreach(callback, {'partition_filter': {'begin': 1001, 'count': 1}})

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    #@pytest.mark.xfail(reason="Might fail, server may return less than what asked for.")
    def test_scan_partition_status_with_existent_ns_and_set(self):

        records = []
        scan_page_size = [5]
        scan_count = [0]
        scan_pages = [5]
        max_records = self.partition_1000_count + \
            self.partition_1001_count + \
            self.partition_1002_count + \
            self.partition_1003_count
        break_count = [5]
        # partition_status = [{id:(id, init, done, digest)},(),...]
        def init(id):
            return 0;
        def done(id):
            return 0;
        def digest(id):
            return bytearray([0]*20);
        partition_status = {id:(id, init(id), done(id), digest(id)) for id in range (1000, 1004,1)}
        partition_filter = {'begin': 1000, 'count': 4, 'partition_status': partition_status}
        # print("scan_obj", partition_filter)
        policy = {'max_records': scan_page_size[0],
                'partition_filter': partition_filter,
                'records_per_second': 4000}
        def callback(part_id, input_tuple):
            if(input_tuple == None):
                # print("callback: NO record")
                return True #scan complete
            (key, _, record) = input_tuple
            partition_status.update({part_id:(part_id, 1, 0, key[3])})
            # print("callback:", part_id, input_tuple, key[3], partition_status.get(part_id));
            records.append(record)
            scan_count[0] = scan_count[0] + 1
            break_count[0] = break_count[0] - 1
            if(break_count[0] == 0):
                return False 

        scan_obj = self.as_connection.scan(self.test_ns, self.test_set)

        i = 0
        for i in range(scan_pages[0]):
            # print("calling scan_obj.foreach")
            scan_obj.foreach(callback, policy)
            if scan_obj.is_done() == True: 
                # print(f"scan completed iter:{i}")
                break
            # print("bc:", break_count[0])
            if(break_count[0] == 0):
                break

        assert len(records) == scan_count[0]

        scan_page_size = [1000]
        scan_count[0] = 0
        break_count[0] = 1000
        partition_filter = {'begin': 1000, 'count': 4, 'partition_status': partition_status}
        # print("new_scan_obj", partition_filter)
        policy = {'max_records': scan_page_size[0],
                'partition_filter': partition_filter,
                'records_per_second': 4000}

        new_scan_obj = self.as_connection.scan(self.test_ns, self.test_set)
        i = 0
        for i in range(scan_pages[0]):
            # print("calling new_scan_obj.foreach")
            new_scan_obj.foreach(callback, policy)
            #assert scan_page_size[0] == scan_count[0]
            if new_scan_obj.is_done() == True: 
                # print(f"scan completed iter:{i}")
                break
            if(break_count[0] == 0):
                break

        assert len(records) == max_records
