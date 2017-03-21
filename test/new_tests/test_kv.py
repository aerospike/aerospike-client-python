import sys
import pytest
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def get_key_with_digest_only(key):
    """
    Takes a key tuple, and removes a key tuple
    without a primary key, but with a key digest
    """
    return (key[0], key[1], None, key[3])


@pytest.mark.usefixtures("as_connection")
class TestKV(object):
    """
    Tests for fetching items by their keys
    """

    def setup_method(self):
        self.record_count = 0

    def test_using_a_single_key(self):
        '''
        Using a single key,
        '''
        key = ("test", "unittest", "1")

        # cleanup records

        self.record_count = 0

        def count_records(tuple):
            self.record_count += 1

        def remove_record(input_tuple):
            key, _, _ = input_tuple
            self.as_connection.remove(key)

        self.as_connection.scan("test", "unittest").foreach(remove_record)

        record_to_insert = {
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
        # create the record and insert it
        status = self.as_connection.put(key, record_to_insert)
        assert status == 0, 'wrong return code'

        # ensure the record exists
        (key, meta) = self.as_connection.exists(key)
        assert meta is not None

        # count inserted records
        self.as_connection.scan("test", "unittest").foreach(count_records)
        assert self.record_count == 1

        # extract a record
        (key, meta, extracted_record) = self.as_connection.get(key)
        assert record_to_insert == extracted_record, 'records do not match'

        # add a new bin with value to the record
        status = self.as_connection.put(key, {"hello": "world"})
        assert status == 0, 'wrong return code'

        # add the same values to our local version of the record
        record_to_insert["hello"] = "world"

        # get the stored record
        (key, meta, extracted_record) = self.as_connection.get(key)
        assert record_to_insert == extracted_record, 'records do not match'

        # remove the record from the DB
        status = self.as_connection.remove(key)
        assert status == 0, 'wrong return code'

        # ensure not existent
        try:
            (key, meta) = self.as_connection.exists(key)
            """
            We are making the api backward compatible. In case of
            RecordNotFound an exception will not be raised.
            Instead Ok response is returned withe the
            meta as None. This might change with further releases.
            """
            assert meta is None
        except e.RecordNotFound as exception:
            assert exception.code == 2

        # count records
        self.record_count = 0
        self.as_connection.scan("test", "unittest").foreach(count_records)
        assert self.record_count == 0, 'set should be empty'

    def test_with_single_key(self):
        '''
        Using a single key, with digest only.
        '''

        self.record_count = 0

        def count_records(tuple):
            self.record_count += 1

        key = ("test", "unittest", "1")

        # cleanup records
        def each_record(input_tuple):
            key, _, _ = input_tuple
            self.as_connection.remove(key)

        self.as_connection.scan("test", "unittest").foreach(each_record)

        record_to_insert = {
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
        status = self.as_connection.put(key, record_to_insert)
        assert status == 0, 'wrong return code'

        # ensure existence
        (key, meta) = self.as_connection.exists(key)
        assert meta is not None

        # count records
        self.record_count = 0
        self.as_connection.scan("test", "unittest").foreach(count_records)
        assert self.record_count == 1, 'set should have 1 record'

        # read it
        (key, meta, extracted_record) = self.as_connection.get(
            get_key_with_digest_only(key))
        assert record_to_insert == extracted_record, 'records do not match'

        # Add a bin to the stored record
        status = self.as_connection.put(
            get_key_with_digest_only(key), {"hello": "world"})
        assert status == 0, 'wrong return code'

        # Add a new value to the local record which matches the one
        # we stored remotely
        record_to_insert["hello"] = "world"

        # fetch the remote key and verify it matches the local version
        (key, meta, extracted_record) = self.as_connection.get(
            get_key_with_digest_only(key))
        assert record_to_insert == extracted_record, 'records do not match'

        # remove it
        status = self.as_connection.remove(
            get_key_with_digest_only(key))
        assert status == 0, 'wrong return code'

        # ensure not existent
        try:
            (key, meta) = self.as_connection.exists(
                get_key_with_digest_only(key))
            # We are making the api backward compatible. In case of
            # RecordNotFound an exception will not be raised.
            # Instead Ok response is returned withe the
            # meta as None. This might change with further releases.
            assert meta is None
        except e.RecordNotFound as exception:
            assert exception.code == 2

        # count records
        self.record_count = 0
        self.as_connection.scan("test", "unittest").foreach(count_records)
        assert self.record_count == 0

    @pytest.mark.skip(reason="Fails intermittently due to nature of callbacks")
    def test_with_multiple_keys(self):
        """
        Using multiple keys
        """
        from aerospike import predicates as p
        records = []

        def count_records_false(tuple):
            records.append(1)
            return False

        for i in range(3):
            key = ('test', 'unittest', i)
            rec = {
                'name': 'name%s' % (str(i)),
                'addr': 'name%s' % (str(i)),
                'age': i,
                'no': i
            }
            self.as_connection.put(key, rec)

        self.as_connection.index_integer_create('test', 'unittest', 'age',
                                                'age_index', {})

        query = self.as_connection.query('test', 'unittest')

        query.select("name", "age")
        self.record_count = 0
        query.where(p.between('age', 1, 3))

        query.foreach(count_records_false)
        self.record_count = len(records)
        # There are two records with age > 1 and < 3, but our callback
        # returns false, so iteration is stopped after the first
        # call
        assert self.record_count == 1, "foreach failed"

        for i in range(3):
            key = ('test', 'unittest', i)
            self.as_connection.remove(key)

        self.as_connection.index_remove('test', 'age_index', {})
