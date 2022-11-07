# These fail if we don't have a new server
# -*- coding: utf-8 -*-
import pytest
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import map_operations as map_ops


def skip_less_than_430(version):
    if version < [4, 3]:
        print(version)
        pytest.skip("Requires server > 4.3.0 to work")

class TestMapWriteFlags(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        self.keys = []

        yield

        for key in self.keys:
            self.as_connection.remove(key)

    def test_default_allows_update_and_create(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_DEFAULT
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
            map_ops.map_put('map', 'new', 'new', map_policy=map_policy),
        ]
        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'new'
        assert map_bin['new'] == 'new'

    def test_create_only_does_not_allow_update(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
        ]
        with pytest.raises(e.AerospikeError):
            self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'

    def test_create_only_allows_create(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'new', 'new', map_policy=map_policy),
        ]
        self.as_connection.operate(key, ops)
        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'
        assert map_bin['new'] == 'new'

    def test_update_only_does_not_allow_create(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'new', 'new', map_policy=map_policy),
        ]

        with pytest.raises(e.AerospikeError):
            self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'
        assert 'new' not in map_bin

    def test_update_only_allows_update(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
        ]

        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'new'

    def test_nofail_allows_an_op_to_fail_silently(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_UPDATE_ONLY | aerospike.MAP_WRITE_FLAGS_NO_FAIL
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
            map_ops.map_put('map', 'new', 'new', map_policy=map_policy),
        ]

        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'new'
        assert 'new' not in map_bin

    def test_partial_allows_partial_write(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': (
                aerospike.MAP_WRITE_FLAGS_CREATE_ONLY |
                aerospike.MAP_WRITE_FLAGS_PARTIAL |
                aerospike.MAP_WRITE_FLAGS_NO_FAIL
            )
        }
        ops = [
            map_ops.map_put_items(
                'map',
                {
                    'existing': 'new',
                    'new1': 'new1',
                    'new2': 'new2'
                }, 
                map_policy=map_policy),
        ]

        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'
        assert map_bin['new1'] == 'new1'
        assert map_bin['new2'] == 'new2'

    def test_no_fail_does_not_allow_partial_write(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': (
                aerospike.MAP_WRITE_FLAGS_CREATE_ONLY |
                aerospike.MAP_WRITE_FLAGS_NO_FAIL
            )
        }
        ops = [
            map_ops.map_put_items(
                'map',
                {
                    'existing': 'new',
                    'new1': 'new1',
                    'new2': 'new2'
                }, 
                map_policy=map_policy),
        ]

        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'
        assert 'new1' not in map_bin
        assert 'new2' not in map_bin


    def test_non_int_write_flag_raises_exception(self):
        skip_less_than_430(self.server_version)
        key = 'test', 'write_flags', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_flags': "waving flag"
        }
        ops = [
            map_ops.map_put_items(
                'map',
                {
                    'existing': 'new',
                    'new1': 'new1',
                    'new2': 'new2'
                }, 
                map_policy=map_policy),
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(key, ops)
