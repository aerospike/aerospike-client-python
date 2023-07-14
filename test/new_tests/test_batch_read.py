import pytest

from aerospike_helpers.batch.records import BatchRecords
from aerospike_helpers.expressions import base as exp
from aerospike import exception as e

from .test_base_class import TestBaseClass
from . import as_errors


class TestBatchRead(TestBaseClass):
    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs

        if self.server_version < [6, 0]:
            pytest.mark.xfail(reason="Servers older than 6.0 do not support batch read.")
            pytest.xfail()

        self.test_ns = "test"
        self.test_set = "demo"
        self.keys = []
        self.keys_to_expected_bins = {}
        self.batch_size = 5

        for i in range(self.batch_size):
            key = ("test", "demo", i)
            rec = {
                "count": i,
                "ilist_bin": [
                    i,
                    1,
                    2,
                    6,
                ],
                "imap_bin": {
                    1: 1,
                    2: 2,
                    3: 6,
                },
            }
            as_connection.put(key, rec)
            self.keys.append(key)
            self.keys_to_expected_bins[key] = rec

        def teardown():
            for i in range(self.batch_size):
                key = ("test", "demo", i)
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_batch_read_with_policy(self):
        # No record will satisfy this expression condition
        expr = exp.Eq(exp.IntBin("count"), 99).compile()
        res: BatchRecords = self.as_connection.batch_read(self.keys, policy={"expressions": expr})
        assert res.result == 0
        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.key[:3] == self.keys[i]  # checking key
            assert batch_rec.record is None
            assert batch_rec.result == as_errors.AEROSPIKE_FILTERED_OUT

    def test_batch_read_all_bins(self):
        res: BatchRecords = self.as_connection.batch_read(self.keys)

        for i, batch_rec in enumerate(res.batch_records):
            assert batch_rec.result == 0
            assert batch_rec.key[:3] == self.keys[i]  # checking key
            assert batch_rec.record[0][:3] == self.keys[i]  # checking key in record
            assert batch_rec.record[2] == self.keys_to_expected_bins[self.keys[i]]

    @pytest.mark.parametrize(
        "bins",
        [
            ["count"],
            ["count", "imap_bin"]
        ]
    )
    def test_batch_read_selected_bins(self, bins):
        res: BatchRecords = self.as_connection.batch_read(self.keys, bins)

        for i, batch_rec in enumerate(res.batch_records):
            key = self.keys[i]
            assert batch_rec.result == 0
            assert batch_rec.key[:3] == key  # checking key
            expected_record = {bin_name: bin_value for bin_name, bin_value in
                               self.keys_to_expected_bins[key].items() if bin_name in bins}
            assert batch_rec.record[0][:3] == key  # checking key in record
            assert batch_rec.record[2] == expected_record

    def test_batch_read_no_bins(self):
        res: BatchRecords = self.as_connection.batch_read(self.keys, [])

        for i, batch_rec in enumerate(res.batch_records):
            key = self.keys[i]
            assert batch_rec.result == 0
            assert batch_rec.key[:3] == key  # checking key
            # Only key and metadata should be returned with no bins
            assert len(batch_rec.record) == 2
            assert batch_rec.record[0][:3] == key  # checking key in record
            assert type(batch_rec.record[1]) == dict

    # Negative tests

    def test_batch_read_invalid_args(self):
        with pytest.raises(TypeError):
            self.as_connection.batch_read()

    def test_batch_read_invalid_key_list(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(1)

        assert excinfo.value.msg == "keys should be a list of aerospike key tuples"

    @pytest.mark.parametrize("invalid_key, err_msg", [
        (1, "key should be an aerospike key tuple"),
        (("test", "demo", 1, 2, 3), "failed to convert key at index: 0"),
    ])
    def test_batch_read_invalid_key(self, invalid_key, err_msg):
        with pytest.raises(e.ParamError) as excinfo:
            keys = [invalid_key]
            self.as_connection.batch_read(keys)

        assert excinfo.value.msg == err_msg

    def test_batch_read_invalid_bin_list(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(self.keys, 1)

        assert excinfo.value.msg == "Bins argument should be a list."

    def test_batch_read_invalid_bin(self):
        with pytest.raises(e.ParamError) as excinfo:
            self.as_connection.batch_read(self.keys, [1])

        assert excinfo.value.msg == "Bin name should be a string or unicode string."
