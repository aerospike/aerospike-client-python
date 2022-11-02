# -*- coding: utf-8 -*-
from aerospike_helpers.batch import records as br
from .test_base_class import TestBaseClass


class TestBatchRecords(TestBaseClass):
    def test_batch_read_all_bins_pos(self):
        """
        Test that batch Reads will allow read_all_bins with no ops
        """

        b = br.Read(("test", "demo", 1), ops=None, read_all_bins=True)

        assert b.read_all_bins

    def test_batch_default_arg_pos(self):
        """
        Test that batch Reads will always use a new batch_records list.
        """

        b = br.Read(("test", "demo", 1), ops=None)

        bwr = br.BatchRecords()

        bwr.batch_records.append(b)

        assert len(bwr.batch_records) == 1

        bwr = br.BatchRecords()

        assert len(bwr.batch_records) == 0
