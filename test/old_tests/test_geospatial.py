
# -*- coding: utf-8 -*-

import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e
from aerospike import predicates as p

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestGeospatial(TestBaseClass):

    pytestmark = pytest.mark.skipif(
        TestBaseClass.has_geo_support() == False,
        reason="Server does not support geospatial data.")

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestGeospatial.client = aerospike.client(config).connect()
        else:
            TestGeospatial.client = aerospike.client(
                config).connect(user, password)
        TestGeospatial.client.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index")
        TestGeospatial.client.index_geo2dsphere_create(
            "test", "demo", "loc_polygon", "loc_polygon_index")
        TestGeospatial.client.index_geo2dsphere_create(
            "test", "demo", "loc_circle", "loc_circle_index")

        TestGeospatial.skip_old_server = True
        versioninfo = TestGeospatial.client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value is not None:
                    versionlist = value[
                        value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 7:
                        TestGeospatial.skip_old_server = False

    def teardown_class(cls):
        TestGeospatial.client.index_remove('test', 'loc_index')
        TestGeospatial.client.index_remove('test', 'loc_polygon_index')
        TestGeospatial.client.index_remove('test', 'loc_circle_index')
        TestGeospatial.client.close()

    def setup_method(self, method):

        self.keys = []
        pre = '{"type": "Point", "coordinates"'
        suf = ']}'
        for i in range(10):
            lng = 1220 - (2 * i)
            lat = 375 + (2 * i)
            key = ('test', 'demo', i)
            s = "{0}: [-{1}.{2}, {3}.{4}{5}".format(
                pre, (lng // 10), (lng % 10), (lat // 10), (lat % 10), suf)
            self.geo_object = aerospike.geojson(s)
            TestGeospatial.client.put(key, {"loc": self.geo_object})
            self.keys.append(key)

        key = ('test', 'demo', 'polygon')
        self.geo_object_polygon = aerospike.GeoJSON(
            {"type": "Polygon",
             "coordinates": [[[-122.500000, 37.000000],
                              [-121.000000, 37.000000],
                              [-121.000000, 38.080000],
                              [-122.500000, 38.080000],
                              [-122.500000, 37.000000]]]})

        TestGeospatial.client.put(
            key, {"loc_polygon": self.geo_object_polygon})
        self.keys.append(key)

        if not TestGeospatial.skip_old_server:
            key = ('test', 'demo', 'circle')
            geo_circle = aerospike.GeoJSON(
                {"type": "AeroCircle", "coordinates": [[-122.0, 37.0], 250.2]})
            TestGeospatial.client.put(key, {"loc_circle": geo_circle})
            self.keys.append(key)

    def teardown_method(self, method):
        """
        Teardown method.
        """
        for key in self.keys:
            TestGeospatial.client.remove(key)

    def test_geospatial_put_get_positive(self):
        """
            Perform a get and put with multiple bins including geospatial bin
        """
        key = ('test', 'demo', 'single_geo_put')
        geo_object_single = aerospike.GeoJSON(
            {"type": "Point", "coordinates": [42.34, 58.62]})
        geo_object_dict = aerospike.GeoJSON(
            {"type": "Point", "coordinates": [56.34, 69.62]})

        TestGeospatial.client.put(key, {"loc": geo_object_single, "int_bin": 2,
                                        "string_bin": "str",
                                        "dict_bin": {"a": 1, "b": 2,
                                                     "geo": geo_object_dict}})

        key, _, bins = TestGeospatial.client.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        TestGeospatial.client.remove(key)

    def test_geospatial_positive_query(self):
        """
            Perform a positive geospatial query for a polygon
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

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
            Perform a positive geospatial query for polygon where all points are
            outside polygon
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

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
            TestGeospatial.client.put(key, {"loc": geo_object})
            keys.append(key)

        TestGeospatial.client.index_geo2dsphere_create(
            "test", None, "loc", "loc_index_no_set")
        records = []
        query = TestGeospatial.client.query("test", None)

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

        TestGeospatial.client.index_remove('test', 'loc_index_no_set')
        for key in keys:
            TestGeospatial.client.remove(key)

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
        query = TestGeospatial.client.query("test", "demo")

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

    def test_geospatial_positive_query_for_circle_with_within_radius_helper(self):
        """
            Perform a positive geospatial query for a circle with helper
        """
        if TestGeospatial.skip_old_server is True:
            pytest.skip(
                "Server does not support apply on AeroCircle for GeoJSON")

        records = []
        query = TestGeospatial.client.query("test", "demo")

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

        key, _, bins = TestGeospatial.client.operate(key, llist)
        self.keys.append(key)
        assert type(bins['write_bin']['no']) == aerospike.GeoJSON
        assert bins['write_bin'][
            'no'].unwrap() == {'coordinates': [43.45, 56.75], 'type': 'Point'}

    def test_geospatial_wrap_positive(self):
        """
            Perform a positive wrap on geospatial data
        """
        self.geo_object.wrap({"type": "Polygon",
                              "coordinates": [[[-122.500000, 37.000000],
                                               [-121.000000, 37.000000],
                                               [-121.000000, 38.080000],
                                               [-122.500000, 38.080000],
                                               [-122.500000, 37.000000]]]})

        assert self.geo_object.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_wrap_positive_with_query(self):
        """
            Perform a positive wrap on geospatial data followed by a query
        """
        geo_object_wrap = aerospike.GeoJSON({"type": "Polygon",
                                             "coordinates": [[
                                                 [-124.500000, 37.000000],
                                                 [-125.000000, 37.000000],
                                                 [-121.000000, 38.080000],
                                                 [-122.500000, 38.080000],
                                                 [-124.500000, 37.000000]]]})

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
        query = TestGeospatial.client.query("test", "demo")
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
        self.geo_object.loads(
            '{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000],\
             [-121.000000, 37.000000], [-121.000000, 38.080000],\
             [-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert self.geo_object.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_loads_positive_with_query(self):
        """
            Perform a positive loads on geoJSON raw string followed by a query
        """
        geo_object_loads = aerospike.GeoJSON({"type": "Polygon",
                                              "coordinates": [[
                                                  [-124.500000, 37.000000],
                                                  [-125.000000, 37.000000],
                                                  [-121.000000, 38.080000],
                                                  [-122.500000, 38.080000],
                                                  [-124.500000, 37.000000]]]})

        geo_object_loads.loads(
            '{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000],\
             [-121.000000, 37.000000], [-121.000000, 38.080000],\
             [-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert geo_object_loads.unwrap() == {
            'coordinates': [[[-122.5, 37.0], [-121.0, 37.0],
                             [-121.0, 38.08], [-122.5, 38.08],
                             [-122.5, 37.0]]], 'type': 'Polygon'}

        records = []
        query = TestGeospatial.client.query("test", "demo")
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
        geojson_str = self.geo_object.dumps()
        assert type(geojson_str) == str
        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == self.geo_object.unwrap()

        geojson_str = str(self.geo_object)
        assert type(geojson_str) == str
        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == self.geo_object.unwrap()

    def test_geospatial_repr_positive(self):
        """
            Perform a positive repr. Verify using eval()
        """
        geojson_str = eval(repr(self.geo_object))
        assert type(geojson_str) == str
        obj = aerospike.geojson(geojson_str)
        assert obj.unwrap() == self.geo_object.unwrap()

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

        TestGeospatial.client.put(key, {"loc": geo_object_single, "int_bin": 2,
                                        "string_bin": "str",
                                        "dict_bin": {"a": 1, "b": 2, "geo":
                                                     geo_object_dict}})

        key, _, bins = TestGeospatial.client.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        TestGeospatial.client.remove(key)

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

        TestGeospatial.client.put(key, {"loc": geo_object_single, "int_bin": 2,
                                        "string_bin": "str",
                                        "dict_bin": {"a": 1, "b": 2, "geo":
                                                     geo_object_dict}})

        key, _, bins = TestGeospatial.client.get(key)

        expected = {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                    "int_bin": 2, "string_bin": "str",
                    "dict_bin": {"a": 1, "b": 2,
                                 "geo": {'coordinates': [56.34, 69.62], 'type':
                                         'Point'}}}
        for b in bins:
            assert b in expected

        TestGeospatial.client.remove(key)

    def test_geospatial_positive_query_with_geodata(self):
        """
            Perform a positive geospatial query for a polygon with geodata
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

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
        query = TestGeospatial.client.query("test", "demo")

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
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0

        status = TestGeospatial.client.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index")

        assert status == 0

    def test_geospatial_2dindex_positive_with_policy(self):
        """
            Perform a positive 2d index creation with policy
        """
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0

        status = TestGeospatial.client.index_geo2dsphere_create(
            "test", "demo", "loc", "loc_index", {"timeout": 2000})

        assert status == 0

    def test_geospatial_positive_query_with_point(self):
        """
            Perform a positive geospatial query for a point
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

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
        query = TestGeospatial.client.query("test", "demo")

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
        query = TestGeospatial.client.query("test", "demo")

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
        query = TestGeospatial.client.query("test", "demo")

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
        query = TestGeospatial.client.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Point", "coordinates":
                                         [-122.000000, 450.200]})

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
        query = TestGeospatial.client.query("test", "demo")

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

    def test_geospatial_object_not_dict_or_string(self):
        """
            The geospatial object is not a dictionary or string
        """
        try:
            aerospike.GeoJSON(1)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Geospatial data should be a dictionary or raw GeoJSON string'

    def test_geospatial_object_non_json_serializable_string(self):
        """
            The geospatial object is not a json serializable string
        """
        try:
            aerospike.GeoJSON("abc")

        except e.ClientError as exception:
            assert exception.code == -1
            assert exception.msg == 'String is not GeoJSON serializable'

    def test_geospatial_object_wrap_non_dict(self):
        """
            The geospatial object provided to wrap() is not a dictionary
        """
        try:
            self.geo_object.wrap("abc")
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Geospatial data should be a dictionary or raw GeoJSON string'

    def test_geospatial_object_loads_non_dict(self):
        """
            The geospatial object provided to loads() is not a dictionary
        """
        try:
            self.geo_object.loads('{"abc"}')
        except e.ClientError as exception:
            assert exception.code == -1
            assert exception.msg == 'String is not GeoJSON serializable'

    def test_geospatial_2dindex_set_length_extra(self):
        """
            Perform a 2d creation with set length exceeding limit
        """
        set_name = 'a'
        for _ in range(100):
            set_name = set_name + 'a'
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0
        try:
            TestGeospatial.client.index_geo2dsphere_create(
                "test", set_name, "loc", "loc_index")

        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_geospatial_query_circle_incorrect_param_within_radius_helper_method(self):
        """
            Perform a positive geospatial query for a circle with incorrect
            params for helper method
        """
        query = TestGeospatial.client.query("test", "demo")

        try:
            query.where(p.geo_contains_point("loc_polygon", -122.0, 37.5, 250))
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "predicate is invalid."
        except SystemError:
            pass

    def test_geospatial_query_point_incorrect_param_helper_method(self):
        """
            Perform a positive geospatial query for a point with incorrect
            params for helper method
        """

        query = TestGeospatial.client.query("test", "demo")

        try:
            query.where(p.geo_contains_point("loc_polygon", -121.7, 37))
        except e.ParamError as exception:
            assert exception.code == -2
