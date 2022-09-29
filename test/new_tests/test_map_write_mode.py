# These fail if we don't have a new server
# -*- coding: utf-8 -*-
import pytest
import aerospike
from aerospike import exception as e
from aerospike_helpers.operations import map_operations as map_ops

class TestMapWriteMode(object):
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
        key = 'test', 'write_mode', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_mode': aerospike.MAP_UPDATE
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
        key = 'test', 'write_mode', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_mode': aerospike.MAP_CREATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
        ]
        with pytest.raises(e.ElementExistsError):
            self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'

    def test_create_only_allows_create(self):
        key = 'test', 'write_mode', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_mode': aerospike.MAP_CREATE_ONLY
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
        key = 'test', 'write_mode', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_mode': aerospike.MAP_UPDATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'new', 'new', map_policy=map_policy),
        ]

        with pytest.raises(e.ElementNotFoundError):
            self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'old'
        assert 'new' not in map_bin

    def test_update_only_allows_update(self):
        key = 'test', 'write_mode', 1
        self.keys.append(key)
        self.as_connection.put(key, {'map': {'existing': 'old'}})

        map_policy = {
            'map_write_mode': aerospike.MAP_UPDATE_ONLY
        }
        ops = [
            map_ops.map_put('map', 'existing', 'new', map_policy=map_policy),
        ]

        self.as_connection.operate(key, ops)

        _, _, bins = self.as_connection.get(key)

        map_bin = bins['map']
        assert map_bin['existing'] == 'new'