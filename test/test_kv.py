import unittest
from test_base_class import TestBaseClass

import aerospike
from aerospike.exception import *

config = {"hosts": [("127.0.0.1", 3000)]}

# count records
count = 0


def count_records((key, meta, rec)):
    global count
    count += 1


def count_records_false((key, meta, rec)):
    global count
    count += 1
    return False


def digest_only(key):
    return (key[0], key[1], None, key[3])


class KVTestCase(unittest.TestCase, TestBaseClass):
    def setup_class(cls):
        KVTestCase.hostlist, KVTestCase.user, KVTestCase.password = TestBaseClass.get_hosts(
        )

    def setUp(self):
        config = {"hosts": KVTestCase.hostlist}
        if KVTestCase.user == None and KVTestCase.password == None:
            self.client = aerospike.client(config).connect()
        else:
            self.client = aerospike.client(config).connect(KVTestCase.user,
                                                           KVTestCase.password)

    def tearDown(self):
        self.client.close()

    def test_1(self):
        '''
        Using a single key, 
        '''

        global count

        key = ("test", "unittest", "1")

        # cleanup records
        def remove_record((key, meta, rec)):
            self.client.remove(key)

        self.client.scan("test", "unittest").foreach(remove_record)

        recIn = {
            "i": 1234,
            "s": "abcd",
            "b": bytearray("efgh", "utf-8"),
            "l": [1357, "aceg", bytearray("aceg", "utf-8"), [1, 3, 5, 7],
                  {"a": 1,
                   "c": 3,
                   "e": 5,
                   "g": 7}],
            "m": {
                "i": 2468,
                "s": "bdfh",
                "l": [2468, "bdfh", bytearray("bdfh", "utf-8")],
                "m": {"b": 2,
                      "d": 4,
                      "f": 6,
                      "h": 8}
            },
        }

        # create the record
        rc = self.client.put(key, recIn)
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure existence
        (key, meta) = self.client.exists(key)
        self.assertTrue(meta is not None)

        # count records
        count = 0
        self.client.scan("test", "unittest").foreach(count_records)
        assert count == 1
        self.assertEqual(count, 1, 'set should have 1 record')

        # read it
        (key, meta, recOut) = self.client.get(key)
        self.assertEqual(recIn, recOut, 'records do not match')

        # create the record
        rc = self.client.put(key, {"hello": "world"})
        self.assertEqual(rc, 0, 'wrong return code')

        # augmented record
        recIn["hello"] = "world"

        # read it
        (key, meta, recOut) = self.client.get(key)
        self.assertEqual(recIn, recOut, 'records do not match')

        # remove it
        rc = self.client.remove(key)
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure not existent
        try:
            (key, meta) = self.client.exists(key)
            """
            We are making the api backward compatible. In case of RecordNotFound an
            exception will not be raised. Instead Ok response is returned withe the
            meta as None. This might change with further releases.
            """
        except RecordNotFound as exception:
            assert True == False
            assert exception.code == 2

        assert meta == None

        # count records
        count = 0
        self.client.scan("test", "unittest").foreach(count_records)
        self.assertEqual(count, 0, 'set should be empty')

    def test_2(self):
        '''
        Using a single key, with digest only.
        '''

        global count

        key = ("test", "unittest", "1")

        # cleanup records
        def each_record((key, meta, rec)):
            self.client.remove(key)

        self.client.scan("test", "unittest").foreach(each_record)

        recIn = {
            "i": 1234,
            "s": "abcd",
            "b": bytearray("efgh", "utf-8"),
            "l": [1357, "aceg", bytearray("aceg", "utf-8"), [1, 3, 5, 7],
                  {"a": 1,
                   "c": 3,
                   "e": 5,
                   "g": 7}],
            "m": {
                "i": 2468,
                "s": "bdfh",
                "l": [2468, "bdfh", bytearray("bdfh", "utf-8")],
                "m": {"b": 2,
                      "d": 4,
                      "f": 6,
                      "h": 8}
            },
            'a': {u'aa': u'11'},
            'k': {u'kk': u'22'}
        }

        # create the record
        rc = self.client.put(key, recIn)
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure existence
        (key, meta) = self.client.exists(key)
        self.assertTrue(meta is not None)

        # count records
        count = 0
        self.client.scan("test", "unittest").foreach(count_records)
        self.assertEqual(count, 1, 'set should have 1 record')

        # read it
        (key, meta, recOut) = self.client.get(digest_only(key))
        self.assertEqual(recIn, recOut, 'records do not match')

        # create the record
        rc = self.client.put(digest_only(key), {"hello": "world"})
        self.assertEqual(rc, 0, 'wrong return code')

        # augmented record
        recIn["hello"] = "world"

        # read it
        (key, meta, recOut) = self.client.get(digest_only(key))
        self.assertEqual(recIn, recOut, 'records do not match')

        # remove it
        rc = self.client.remove(digest_only(key))
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure not existent
        try:
            (key, meta) = self.client.exists(digest_only(key))
            """
            We are making the api backward compatible. In case of RecordNotFound an
            exception will not be raised. Instead Ok response is returned withe the
            meta as None. This might change with further releases.
            """
        except RecordNotFound as exception:
            assert True == False
            assert exception.code == 2

        assert meta == None

        # count records
        count = 0
        self.client.scan("test", "unittest").foreach(count_records)
        self.assertEqual(count, 0, 'set should be empty')

    def test_3(self):
        """
        Using multiple keys
        """
        from aerospike import predicates as p
        from time import sleep
        global count

        for i in xrange(2):
            key = ('test', 'unittest', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i
            }
            self.client.put(key, rec)

        self.client.index_integer_create('test', 'unittest', 'age',
                                         'age_index', {})

        query = self.client.query('test', 'unittest')

        query.select("name", "age")
        count = 0
        query.where(p.between('age', 1, 3))

        query.foreach(count_records_false)

        self.assertEqual(count, 1, "foreach failed")

        for i in xrange(2):
            key = ('test', 'unittest', i)
            self.client.remove(key)

        self.client.index_remove('test', 'age_index', {})


suite = unittest.TestLoader().loadTestsFromTestCase(KVTestCase)
