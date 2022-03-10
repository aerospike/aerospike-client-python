# -*- coding: utf-8 -*-

import sys

import pytest

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op
from .test_base_class import TestBaseClass

class TestBatchRecords(TestBaseClass):
    def test_batch_read_all_bins_pos(self):
        """
        Test that BatchReads will allow read_all_bins with no ops
        """

        b = br.BatchRead(
            ("test", "demo", 1),
            ops = None,
            read_all_bins=True
        )

        assert b.read_all_bins
