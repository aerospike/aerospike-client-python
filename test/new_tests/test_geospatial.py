
# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e
from aerospike import predicates as p
import time

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def get_geo_object():
        geo_object = aerospike.GeoJSON({"type": "Polygon",
                                        "coordinates": [[
                                             [-124.500000, 37.000000],
                                             [-125.000000, 37.000000],
                                             [-121.000000, 38.080000],
                                             [-122.500000, 38.080000],
                                             [-124.500000, 37.000000]]]})
        return geo_object


def add_geo_indexes(connection):
    try:
        connection.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_geo2dsphere_create(
            "test", "demo", "loc_polygon", "loc_polygon_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_geo2dsphere_create(
            "test", "demo", "loc_circle", "loc_circle_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_list_create(
            "test", "demo", "geo_list", aerospike.INDEX_GEO2DSPHERE,
            "geo_list_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_map_keys_create(
            "test", "demo", "geo_map_keys", aerospike.INDEX_GEO2DSPHERE,
            "geo_map_key_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_map_values_create(
            "test", "demo", "geo_map_vals", aerospike.INDEX_GEO2DSPHERE,
            "geo_map_val_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_list_create(
            "test", "demo", "geo_loc_list", aerospike.INDEX_GEO2DSPHERE,
            "geo_loc_list_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_map_keys_create(
            "test", "demo", "geo_loc_mk", aerospike.INDEX_GEO2DSPHERE,
            "geo_loc_map_key_index")
    except(e.IndexFoundError):
        pass

    try:
        connection.index_map_values_create(
            "test", "demo", "geo_loc_mv", aerospike.INDEX_GEO2DSPHERE,
            "geo_loc_map_val_index")
    except(e.IndexFoundError):
        pass


def add_geo_data(connection):
    pre = '{"type": "Point", "coordinates"'
    suf = ']}'
    for i in range(10):
        lng = 1220 - (2 * i)
        lat = 375 + (2 * i)
        key = ('test', 'demo', i)
        s = "{0}: [-{1}.{2}, {3}.{4}{5}".format(
            pre, (lng // 10), (lng % 10), (lat // 10), (lat % 10), suf)
        geo_object = aerospike.geojson(s)
        geo_list = [geo_object]
        geo_map_key = {geo_object: i}
        geo_map_val = {i: geo_object}
        connection.put(
            key,
            {
                "loc": geo_object,
                'geo_list': geo_list,
                'geo_map_keys': geo_map_key,
                'geo_map_vals': geo_map_val
            }
        )

    key = ('test', 'demo', 'polygon')
    geo_object_polygon = aerospike.GeoJSON(
        {"type": "Polygon",
         "coordinates": [[[-122.500000, 37.000000],
                          [-121.000000, 37.000000],
                          [-121.000000, 38.080000],
                          [-122.500000, 38.080000],
                          [-122.500000, 37.000000]]]})

    geo_loc_list = [geo_object_polygon]
    geo_loc_mk = {geo_object_polygon: 1}
    geo_loc_mv = {2: geo_object_polygon}
    connection.put(
        key,
        {
            "loc_polygon": geo_object_polygon,
            'geo_loc_list': geo_loc_list,
            'geo_loc_mk': geo_loc_mk,
            'geo_loc_mv': geo_loc_mv
        }
    )

    key = ('test', 'demo', 'polygon2')
    geo_object_polygon = aerospike.GeoJSON(
        {"type": "Polygon",
         "coordinates": [[[-52.500000, 37.000000],
                          [-51.000000, 37.000000],
                          [-51.000000, 38.080000],
                          [-52.500000, 38.080000],
                          [-52.500000, 37.000000]]]})

    geo_loc_list = [geo_object_polygon]
    geo_loc_mk = {geo_object_polygon: 1}
    geo_loc_mv = {2: geo_object_polygon}
    connection.put(
        key,
        {
            "loc_polygon": geo_object_polygon,
            'geo_loc_list': geo_loc_list,
            'geo_loc_mk': geo_loc_mk,
            'geo_loc_mv': geo_loc_mv
        }
    )


def remove_geo_indexes(connection):
    try:
        connection.index_remove('test', 'loc_index')
    except:
        pass

    try:
        connection.index_remove('test', 'loc_polygon_index')
    except:
        pass

    try:
        connection.index_remove('test', 'loc_circle_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_list_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_map_key_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_map_val_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_loc_list_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_loc_map_key_index')
    except:
        pass

    try:
        connection.index_remove('test', 'geo_loc_map_val_index')
    except:
        pass


def remove_geo_data(connection):
    for i in range(10):
        key = ('test', 'demo', i)
        connection.remove(key)

    key = ('test', 'demo', 'polygon')
    connection.remove(key)

    key = ('test', 'demo', 'polygon2')
    connection.remove(key)


class TestGeospatial(object):

    def setup_class(cls):
        """
        Setup method.
        """
        cls.connection_setup_functions = (
            add_geo_indexes,
            add_geo_data
        )

        cls.connection_teardown_functions = (
            remove_geo_indexes,
            remove_geo_data
        )

    @pytest.fixture(autouse=True)
    def setup(self, request, connection_with_config_funcs):
        as_connection = connection_with_config_funcs
        self.keys = []
        if not self.skip_old_server:
            key = ('test', 'demo', 'circle')
            geo_circle = aerospike.GeoJSON(
                {"type": "AeroCircle", "coordinates": [[-122.0, 37.0], 250.2]})
            as_connection.put(key, {"loc_circle": geo_circle})
            self.keys.append(key)

        def teardown():
            for key in self.keys:
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_geospatial_put_get_positive(self):
        """
            Perform a get and put with multiple bins including geospatial bin
        """
        key = ('test', 'demo', 'single_geo_put')
        geo_object_single = aerospike.GeoJSON(
            {"type": "Point", "coordinates": [42.34, 58.62]})
        geo_object_dict = aerospike.GeoJSON(
            {"type": "Point", "coordinates": [56.34, 69.62]})

        self.as_connection.put(key, {"loc": geo_object_single, "int_bin": 2,
                                     "string_bin": "str",
                                     "dict_bin": {"a": 1, "b": 2,
                                                  "geo": geo_object_dict}})

        key, _, bins = self.as_connection.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        self.as_connection.remove(key)

    def test_geospatial_positive_query(self):
        """
            Perform a positive geospatial query for a polygon
        """
        records = []
        query = self.as_connection.query("test", "demo")
        geo_object2 = aerospike.GeoJSON({"type": "Polygon",
                                         "coordinates": [[
                                             [-122.500000, 37.000000],
                                             [-121.000000, 37.000000],
                                             [-121.000000, 38.080000],
                                             [-122.500000, 38.080000],
                                             [-122.500000, 37.000000]]]})

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        expected = [{"coordinates": [-122.0, 37.5], "type": "Point"},
                    {"coordinates": [-121.8, 37.7], "type": "Point"},
                    {"coordinates": [-121.6, 37.9], "type": "Point"}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_positive_query_outside_shape(self):
        """
            Perform a positive geospatial query for polygon where all points
            are outside polygon
        """
        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Polygon",
                                         "coordinates": [[
                                             [-126.500000, 37.000000],
                                             [-124.000000, 37.000000],
                                             [-124.000000, 38.080000],
                                             [-126.500000, 38.080000],
                                             [-126.500000, 37.000000]]]})

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 0

    def test_geospatial_positive_query_without_set(self):
        """
            Perform a positive geospatial query for a polygon without a set
        """
        keys = []
        pre = '{"type": "Point", "coordinates"'
        suf = ']}'
        for i in range(1, 10):
            lng = 1220 - (2 * i)
            lat = 375 + (2 * i)
            key = ('test', None, i)
            s = "{0}: [-{1}.{2}, {3}.{4}{5}".format(
                pre, (lng // 10), (lng % 10), (lat // 10), (lat % 10), suf)
            geo_object = aerospike.geojson(s)
            self.as_connection.put(key, {"loc": geo_object})
            keys.append(key)

        try:
            self.as_connection.index_geo2dsphere_create(
                "test", None, "loc", "loc_index_no_set")
        except(e.IndexFoundError):
            pass

        records = []
        query = self.as_connection.query("test", None)

        geo_object2 = aerospike.GeoJSON({"type": "Polygon",
                                         "coordinates": [[
                                             [-122.500000, 37.000000],
                                             [-121.000000, 37.000000],
                                             [-121.000000, 38.080000],
                                             [-122.500000, 38.080000],
                                             [-122.500000, 37.000000]]]})

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)
        try:
            self.as_connection.index_remove('test', 'loc_index_no_set')
        except(Exception):
            pass

        for key in keys:
            self.as_connection.remove(key)

        assert len(records) == 2
        expected = [{'coordinates': [-121.8, 37.7], 'type': 'Point'},
                    {'coordinates': [-121.6, 37.9], 'type': 'Point'}]

        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_positive_query_for_circle(self):
        """
            Perform a positive geospatial query for a circle
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON(
            {"type": "AeroCircle", "coordinates": [[-122.0, 37.5], 250.2]})

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geo_query_with_geo_within_radius_predicate(self):
        """
            Perform a positive geospatial query for a circle with helper
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = self.as_connection.query("test", "demo")

        query.where(p.geo_within_radius("loc", -122.0, 37.5, 250.2))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_operate_positive(self):
        """
            Perform an operate operation with geospatial bin
        """

        geo_object_operate = aerospike.GeoJSON(
            {"type": "Point", "coordinates": [43.45, 56.75]})
        key = ('test', 'demo', 'single_geo_operate')
        llist = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": geo_object_operate}
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, _, bins = self.as_connection.operate(key, llist)
        self.keys.append(key)
        assert type(bins['write_bin']['no']) == aerospike.GeoJSON
        assert bins['write_bin'][
            'no'].unwrap() == {'coordinates': [43.45, 56.75], 'type': 'Point'}

    def test_geospatial_wrap_positive(self):
        """
            Perform a positive wrap on geospatial data
        """
        geo_object = aerospike.GeoJSON(
                                      {
                                          "type": "Polygon",
                                          "coordinates": [[
                                              [-124.500000, 37.000000],
                                              [-125.000000, 37.000000],
                                              [-121.000000, 38.080000],
                                              [-122.500000, 38.080000],
                                              [-124.500000, 37.000000]]]
                                      })
        geo_object.wrap({"type": "Polygon",
                        "coordinates": [[[-122.500000, 37.000000],
                                        [-121.000000, 37.000000],
                                        [-121.000000, 38.080000],
                                        [-122.500000, 38.080000],
                                        [-122.500000, 37.000000]]]})

        assert geo_object.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_wrap_positive_with_query(self):
        """
            Perform a positive wrap on geospatial data followed by a query
        """
        geo_object_wrap = get_geo_object()

        geo_object_wrap.wrap({"type": "Polygon",
                              "coordinates": [[[-122.500000, 37.000000],
                                               [-121.000000, 37.000000],
                                               [-121.000000, 38.080000],
                                               [-122.500000, 38.080000],
                                               [-122.500000, 37.000000]]]})

        assert geo_object_wrap.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

        records = []
        query = self.as_connection.query("test", "demo")
        query.where(
            p.geo_within_geojson_region("loc", geo_object_wrap.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'},
                    {'coordinates': [-121.8, 37.7], 'type':'Point'},
                    {'coordinates': [-121.6, 37.9], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_loads_positive(self):
        """
            Perform a positive loads on geoJSON raw string
        """
        geo_object = get_geo_object()
        geo_object.loads(
            '{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000],\
             [-121.000000, 37.000000], [-121.000000, 38.080000],\
             [-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert geo_object.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_loads_positive_with_query(self):
        """
            Perform a positive loads on geoJSON raw string followed by a query
        """
        geo_object_loads = get_geo_object()

        geo_object_loads.loads(
            '{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000],\
             [-121.000000, 37.000000], [-121.000000, 38.080000],\
             [-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert geo_object_loads.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

        records = []
        query = self.as_connection.query("test", "demo")
        query.where(
            p.geo_within_geojson_region("loc", geo_object_loads.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'},
                    {'coordinates': [-121.8, 37.7], 'type': 'Point'},
                    {'coordinates': [-121.6, 37.9], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_dumps_positive(self):
        """
            Perform a positive dumps. Verify using str
        """
        geo_object = get_geo_object()

        geojson_str = geo_object.dumps()
        assert isinstance(geojson_str, str)

        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == geo_object.unwrap()

    def test_geojson_str(self):
        """
        verify that str representation of geojson object is correct
        """
        geo_object = get_geo_object()

        geojson_str = str(geo_object)
        assert isinstance(geojson_str, str)

        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == geo_object.unwrap()

    def test_geospatial_repr_positive(self):
        """
            Perform a positive repr. Verify using eval()
        """

        geo_object = get_geo_object()

        geojson_str = eval(repr(geo_object))
        assert isinstance(geojson_str, str)

        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == geo_object.unwrap()

    def test_geospatial_put_get_positive_with_geodata(self):
        """
            Perform a get and put with multiple bins including geospatial bin
            using geodata method
        """

        key = ('test', 'demo', 'single_geo_put')
        geo_object_single = aerospike.geodata(
            {"type": "Point", "coordinates": [42.34, 58.62]})
        geo_object_dict = aerospike.geodata(
            {"type": "Point", "coordinates": [56.34, 69.62]})

        self.as_connection.put(key, {
                                     "loc": geo_object_single,
                                     "int_bin": 2,
                                     "string_bin": "str",
                                     "dict_bin": {
                                         "a": 1, "b": 2,
                                         "geo": geo_object_dict
                                     }
                                     })

        key, _, bins = self.as_connection.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        self.as_connection.remove(key)

    def test_geospatial_put_get_positive_with_geojson(self):
        """
            Perform a get and put with multiple bins including geospatial bin
            using geodata method
        """

        key = ('test', 'demo', 'single_geo_put')
        geo_object_single = aerospike.geojson(
            '{"type": "Point", "coordinates": [42.34, 58.62] }')
        geo_object_dict = aerospike.geojson(
            '{"type": "Point", "coordinates": [56.34, 69.62] }')

        self.as_connection.put(key, {"loc": geo_object_single, "int_bin": 2,
                                     "string_bin": "str",
                                     "dict_bin": {"a": 1, "b": 2, "geo":
                                                  geo_object_dict}})

        key, _, bins = self.as_connection.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        self.as_connection.remove(key)

    def test_geospatial_positive_query_with_geodata(self):
        """
            Perform a positive geospatial query for a polygon with geodata
        """
        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.geodata({"type": "Polygon",
                                         "coordinates": [[
                                             [-122.500000, 37.000000],
                                             [-121.000000, 37.000000],
                                             [-121.000000, 38.080000],
                                             [-122.500000, 38.080000],
                                             [-122.500000, 37.000000]]]})

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'},
                    {'coordinates': [-121.8, 37.7], 'type': 'Point'},
                    {'coordinates': [-121.6, 37.9], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_positive_query_with_geojson(self):
        """
            Perform a positive geospatial query for a polygon with geojson
        """
        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.geojson(
            '{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000], \
            [-121.000000, 37.000000], [-121.000000, 38.080000],\
            [-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        query.where(p.geo_within_geojson_region("loc", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        expected = [{'coordinates': [-122.0, 37.5], 'type': 'Point'},
                    {'coordinates': [-121.8, 37.7], 'type': 'Point'},
                    {'coordinates': [-121.6, 37.9], 'type': 'Point'}]
        for r in records:
            assert r['loc'].unwrap() in expected

    def test_geospatial_2dindex_positive(self):
        """
            Perform a positive 2d index creation
        """
        try:
            status = self.as_connection.index_remove('test', 'loc_index')
            time.sleep(2)
        except:
            pass

        status = self.as_connection.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index")

        assert status == 0

    def test_geospatial_2dindex_positive_with_policy(self):
        """
            Perform a positive 2d index creation with policy
        """
        try:
            status = self.as_connection.index_remove('test', 'loc_index')
            time.sleep(2)
        except:
            pass

        status = self.as_connection.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index", {"timeout": 2000})

        assert status == 0

    def test_geospatial_positive_query_with_point(self):
        """
            Perform a positive geospatial query for a point
        """
        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-121.700000, 37.200000]})

        query.where(
            p.geo_contains_geojson_point("loc_polygon", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [{'coordinates': [[[-122.500000, 37.000000],
                                      [-121.000000, 37.000000],
                                      [-121.000000, 38.080000],
                                      [-122.500000, 38.080000],
                                      [-122.500000, 37.000000]]],
                     'type': 'Polygon'}]

        for r in records:
            assert r['loc_polygon'].unwrap() in expected

    def test_geospatial_positive_query_with_point_outside_polygon(self):
        """
            Perform a positive geospatial query for a point outside polygon
        """
        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-123.700000, 37.200000]})

        query.where(
            p.geo_contains_geojson_point("loc_polygon", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 0

    def test_geospatial_positive_query_with_point_in_aerocircle(self):
        """
            Perform a positive geospatial query for a point in aerocircle
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-122.000000, 37.000000]})

        query.where(
            p.geo_contains_geojson_point("loc_circle", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [
            {'coordinates': [[-122.0, 37.0], 250.2], 'type': 'AeroCircle'}]
        for r in records:
            assert r['loc_circle'].unwrap() in expected

    def test_geospatial_positive_query_with_point_in_aerocircle_int(self):
        """
            Perform a positive geospatial query for a point in aerocircle
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-122, 37]})

        query.where(
            p.geo_contains_geojson_point("loc_circle", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [
            {'coordinates': [[-122.0, 37.0], 250.2], 'type': 'AeroCircle'}]
        for r in records:
            assert r['loc_circle'].unwrap() in expected

    def test_geospatial_positive_query_with_point_outside_aerocircle(self):
        """
            Perform a positive geospatial query for a point in aerocircle
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-122.0, 48.0]})

        query.where(
            p.geo_contains_geojson_point("loc_circle", geo_object2.dumps()))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 0

    def test_geospatial_positive_query_with_point_helper_method(self):
        """
            Perform a positive geospatial query for a point with helper method
        """
        records = []
        query = self.as_connection.query("test", "demo")

        query.where(p.geo_contains_point("loc_polygon", -121.7, 37.2))

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        expected = [{'coordinates': [[[-122.500000, 37.000000],
                                      [-121.000000, 37.000000],
                                      [-121.000000, 38.080000],
                                      [-122.500000, 38.080000],
                                      [-122.500000, 37.000000]]],
                     'type': 'Polygon'}]

        for r in records:
            assert r['loc_polygon'].unwrap() in expected

    @pytest.mark.parametrize(
        "bin_name, idx_type",
        (
            ('geo_list', aerospike.INDEX_TYPE_LIST),
            ('geo_map_keys', aerospike.INDEX_TYPE_MAPKEYS),
            ('geo_map_vals', aerospike.INDEX_TYPE_MAPVALUES)
        )
    )
    def test_geospatial_within_radius_pred(self, bin_name, idx_type):

        records = []
        query = self.as_connection.query("test", "demo")

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        predicate = p.geo_within_radius(
            bin_name, -122.0, 37.5, 250.2, idx_type)

        query.where(predicate)
        query.foreach(callback)

        assert len(records) == 1

    @pytest.mark.parametrize(
        "bin_name, idx_type",
        (
            ('geo_list', aerospike.INDEX_TYPE_LIST),
            ('geo_map_keys', aerospike.INDEX_TYPE_MAPKEYS),
            ('geo_map_vals', aerospike.INDEX_TYPE_MAPVALUES)
        )
    )
    def test_geospatial_within_geojson_region_pred(self, bin_name, idx_type):

        records = []
        query = self.as_connection.query("test", "demo")

        geo_object2 = aerospike.geodata({"type": "Polygon",
                                         "coordinates": [[
                                             [-122.500000, 37.000000],
                                             [-121.000000, 37.000000],
                                             [-121.000000, 38.080000],
                                             [-122.500000, 38.080000],
                                             [-122.500000, 37.000000]]]})

        predicate = p.geo_within_geojson_region(
            bin_name, geo_object2.dumps(), idx_type)

        query.where(predicate)

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3

    def test_store_multipolygon(self):

        polygons = [

                    [[
                         [-124.500000, 37.000000],
                         [-125.000000, 37.000000],
                         [-121.000000, 38.080000],
                         [-122.500000, 38.080000],
                         [-124.500000, 37.000000]
                    ]],
                    [[
                         [-24.500000, 37.000000],
                         [-25.000000, 37.000000],
                         [-21.000000, 38.080000],
                         [-22.500000, 38.080000],
                         [-24.500000, 37.000000]
                    ]]
                 ]
        geo_object = aerospike.GeoJSON(
            {
                "type": "MultiPolygon",
                "coordinates": polygons
            }
        )

        key = ('test', 'demo', 'multipoly')
        self.as_connection.put(key, {'multi': geo_object})

        _, _, bins = self.as_connection.get(key)

        geo_returned = bins['multi'].unwrap()
        assert geo_returned['type'] == 'MultiPolygon'
        assert geo_returned['coordinates'] == polygons

        self.as_connection.remove(key)

    @pytest.mark.parametrize(
        "bin_name, idx_type",
        (
            ('geo_loc_list', aerospike.INDEX_TYPE_LIST),
            ('geo_loc_mk', aerospike.INDEX_TYPE_MAPKEYS),
            ('geo_loc_mv', aerospike.INDEX_TYPE_MAPVALUES)
        )
    )
    def test_geospatial_contains_point_pred(self, bin_name, idx_type):

        records = []
        query = self.as_connection.query("test", "demo")
        lat = -122.45
        lon = 37.5

        predicate = p.geo_contains_point(
            bin_name, lat, lon, idx_type)

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.where(predicate)
        query.foreach(callback)

        assert len(records) == 1

    @pytest.mark.parametrize(
        "bin_name, idx_type",
        (
            ('geo_loc_list', aerospike.INDEX_TYPE_LIST),
            ('geo_loc_mk', aerospike.INDEX_TYPE_MAPKEYS),
            ('geo_loc_mv', aerospike.INDEX_TYPE_MAPVALUES)
        )
    )
    def test_geospatial_contains_json_point_pred(self, bin_name, idx_type):

        records = []
        query = self.as_connection.query("test", "demo")
        lat = -122.45
        lon = 37.5
        point_list = [lat, lon]

        point = aerospike.GeoJSON({'type': "Point",
                                   'coordinates': point_list})

        predicate = p.geo_contains_geojson_point(
            bin_name, point.dumps(), idx_type)

        def callback(input_tuple):
            _, _, record = input_tuple
            records.append(record)

        query.where(predicate)
        query.foreach(callback)

        assert len(records) == 1

    def test_geospatial_object_not_dict_or_string(self):
        """
            The geospatial object is not a dictionary or string
        """
        with pytest.raises(e.ParamError) as err_info:
            aerospike.GeoJSON(1)

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_geospatial_object_non_json_serializable_string(self):
        """
            The geospatial object is not a json serializable string
        """
        with pytest.raises(e.ClientError) as err_info:
            aerospike.GeoJSON("abc")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_geospatial_object_wrap_non_dict(self):
        """
            The geospatial object provided to wrap() is not a dictionary
        """
        geo_object = get_geo_object()
        with pytest.raises(e.ParamError) as err_info:
            geo_object.wrap("abc")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_geospatial_object_loads_non_dict(self):
        """
            The geospatial object provided to loads() is not a dictionary
        """
        geo_object = get_geo_object()
        with pytest.raises(e.ClientError) as err_info:
            geo_object.loads('{"abc"}')

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_geospatial_2dindex_set_length_extra(self):
        """
            Perform a 2d creation with set length exceeding limit
        """
        set_name = 'a' * 100

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_geo2dsphere_create(
                "test", set_name, "loc", "loc_index_creation_should_fail")

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    @pytest.mark.skip(reason="These raise system errors")
    @pytest.mark.parametrize(
        "method",
        [
            'geo_within_geojson_region',
            'geo_contains_geojson_point',
            'geo_within_radius',
            'geo_contains_point'
        ])
    def test_call_geo_predicates_with_wrong_args(self, method):
        query = self.as_connection.query("test", "demo")
        predicate = getattr(p, method)

        with pytest.raises(e.ParamError) as err_info:
            query.where(predicate())

        err_code = err_info.value.code
        assert err_code == AerospikeStatus.AEROSPIKE_ERR_PARAM
