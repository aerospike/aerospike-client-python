import unittest
import aerospike

config = {
    "hosts": [("localhost",3000)]
}

# count records
count = 0
def count_records((key, meta, rec)):
    global count
    count += 1

def digest_only(key):
    return (key[0], key[1], None, key[3])

class KVTestCase(unittest.TestCase):
    def setUp(self):
        self.client = aerospike.client(config).connect()

    def tearDown(self):
        self.client.close()

    def test_1(self):
        '''
        Using a single key, 
        '''

        global count

        key = ("test","unittest","1")

        # cleanup records
        def remove_record((key, meta, rec)):
            self.client.remove(key)
        self.client.scan("test","unittest").foreach(remove_record)

        recIn = {
            "i": 1234,
            "s": "abcd",
            "b": bytearray("efgh","utf-8"),
            "l": [1357, "aceg", bytearray("aceg","utf-8"), [1,3,5,7], {"a": 1, "c": 3, "e": 5, "g": 7}],
            "m": {"i": 2468, "s": "bdfh", "l": [2468, "bdfh", bytearray("bdfh","utf-8")], "m": {"b": 2, "d": 4, "f": 6, "h": 8}},
            "n": {"t" : u"\ud83d\ude04"},
            "o": u"\ud83d\ude04"
        }

        # create the record
        rc = self.client.put(key, recIn)
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure existence
        (key, meta) = self.client.exists(key)
        self.assertIsNotNone(meta, 'record should exist')

        # count records
        count = 0
        self.client.scan("test","unittest").foreach(count_records)
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
        (key, meta) = self.client.exists(key)
        self.assertIsNone(meta, 'record should not exist')

        # count records
        count = 0
        self.client.scan("test","unittest").foreach(count_records)
        self.assertEqual(count, 0, 'set should be empty')


    def test_2(self):
        '''
        Using a single key, with digest only.
        '''

        global count

        key = ("test","unittest","1")

        # cleanup records
        def each_record((key, meta, rec)):
            self.client.remove(key)
        self.client.scan("test","unittest").foreach(each_record)

        recIn = {
            "i": 1234,
            "s": "abcd",
            "b": bytearray("efgh","utf-8"),
            "l": [1357, "aceg", bytearray("aceg","utf-8"), [1,3,5,7], {"a": 1, "c": 3, "e": 5, "g": 7}],
            "m": {"i": 2468, "s": "bdfh", "l": [2468, "bdfh", bytearray("bdfh","utf-8")], "m": {"b": 2, "d": 4, "f": 6, "h": 8}}
        }
        
        # create the record
        rc = self.client.put(key, recIn)
        self.assertEqual(rc, 0, 'wrong return code')

        # ensure existence
        (key, meta) = self.client.exists(key)
        self.assertIsNotNone(meta, 'record should exist')

        # count records
        count = 0
        self.client.scan("test","unittest").foreach(count_records)
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
        (key, meta) = self.client.exists(digest_only(key))
        self.assertIsNone(meta, 'record should not exist')

        # count records
        count = 0
        self.client.scan("test","unittest").foreach(count_records)
        self.assertEqual(count, 0, 'set should be empty')



suite = unittest.TestLoader().loadTestsFromTestCase(KVTestCase)
