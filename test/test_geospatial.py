
# -*- coding: utf-8 -*-

import pytest
import sys
from test_base_class import TestBaseClass
from aerospike import predicates as p

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
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
        if user == None and password == None:
            TestGeospatial.client = aerospike.client(config).connect()
        else:
            TestGeospatial.client = aerospike.client(config).connect(user, password)
        TestGeospatial.client.index_geo2dsphere_create("test", "demo", "loc", "loc_index")

    def teardown_class(cls):
        TestGeospatial.client.index_remove('test', 'loc_index')
        TestGeospatial.client.close()

    def setup_method(self, method):

        self.keys = []
        for i in xrange(10):
            key = ('test', 'demo', i)
            lng = -122 + (0.2 * i)
            lat = 37.5 + (0.2 * i)
            self.geo_object = aerospike.geojson({"type": "Point", "coordinates": [lng, lat] })
    
            TestGeospatial.client.put(key, {"loc": self.geo_object})
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
        geo_object_single = aerospike.GeoJSON({"type": "Point", "coordinates": [42.34, 58.62] })
        geo_object_dict = aerospike.GeoJSON({"type": "Point", "coordinates": [56.34, 69.62] })
        
        TestGeospatial.client.put(key, {"loc": geo_object_single, "int_bin": 2,
            "string_bin": "str", "dict_bin": {"a": 1, "b":2, "geo":
                geo_object_dict}})
        
        key, meta, bins = TestGeospatial.client.get(key)

        assert bins == {'loc': {'coordinates': [42.34, 58.62], 'type': 'Point'},
                "int_bin": 2, "string_bin": "str", "dict_bin": {"a": 1, "b": 2,
                  "geo": {'coordinates': [56.34, 69.62], 'type':
                        'Point'}}}

        TestGeospatial.client.remove(key)

    def test_geospatial_positive_query(self):
        """
            Perform a positive geospatial query for a polygon
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Polygon", "coordinates": [[[-122.500000,
    37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
        38.080000], [-122.500000, 37.000000]]]})

        query.where(p.within("loc", geo_object2.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        assert records == [{'loc': {'coordinates': [-122.0, 37.5], 'type': 'Point'}}, {'loc': {'coordinates': [-121.8, 37.7], 'type':
                'Point'}}, {'loc': {'coordinates': [-121.6, 37.9], 'type': 'Point'}}]

    def test_geospatial_positive_query_outside_shape(self):
        """
            Perform a positive geospatial query for polygon where all points are
            outside polygon
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Polygon", "coordinates": [[[-126.500000,
    37.000000],[-124.000000, 37.000000], [-124.000000, 38.080000],[-126.500000,
        38.080000], [-126.500000, 37.000000]]]})

        query.where(p.within("loc", geo_object2.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        assert len(records) == 0

    def test_geospatial_positive_query_without_set(self):
        """
            Perform a positive geospatial query for a polygon without a set
        """
        keys = []
        for i in xrange(1, 10):
            key = ('test', None, i)
            lng = -122 + (0.2 * i)
            lat = 37.5 + (0.2 * i)
            geo_object = aerospike.GeoJSON({"type": "Point", "coordinates": [lng, lat] })
    
            TestGeospatial.client.put(key, {"loc": geo_object})
            keys.append(key)

        TestGeospatial.client.index_geo2dsphere_create("test", None, "loc", "loc_index_no_set")
        records = []
        query = TestGeospatial.client.query("test", None)

        geo_object2 = aerospike.GeoJSON({"type": "Polygon", "coordinates": [[[-122.500000,
    37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
        38.080000], [-122.500000, 37.000000]]]})

        query.where(p.within("loc", geo_object2.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        TestGeospatial.client.index_remove('test', 'loc_index_no_set')
        for key in keys:
            TestGeospatial.client.remove(key)

        assert len(records) == 2
        assert records == [{'loc': {'coordinates': [-121.8, 37.7], 'type': 'Point'}}, {'loc': {'coordinates': [-121.6, 37.9], 'type': 'Point'}}]

    def test_geospatial_positive_query_for_circle(self):
        """
            Perform a positive geospatial query for a circle
        """
        records = []
        query = TestGeospatial.client.query("test", "demo")

        geo_object2 = aerospike.GeoJSON({"type": "Circle", "coordinates": [[-122.0, 37.5], 250.2]})

        query.where(p.within("loc", geo_object2.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        assert len(records) == 1
        assert records == [{'loc': {'coordinates': [-122.0, 37.5], 'type': 'Point'}}]

    def test_geospatial_operate_positive(self):
        """
            Perform an operate operation with geospatial bin
        """

        geo_object_operate = aerospike.GeoJSON({"type": "Point", "coordinates": [43.45, 56.75] })
        key = ('test', 'demo', 'single_geo_operate')
        list = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": geo_object_operate}
            }, {"op": aerospike.OPERATOR_READ,
                "bin": "write_bin"}]

        key, meta, bins = TestGeospatial.client.operate(key, list)

        assert bins == {'write_bin': {'no': {'coordinates': [43.45, 56.75],
            'type': 'Point'}}}

        TestGeospatial.client.remove(key)

    def test_geospatial_object_non_dict(self):
        """
            The geospatial object is not a dictionary
        """
        try:
            geo_object_wrong = aerospike.GeoJSON("abc")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Geospatial data should be a dictionary'

    def test_geospatial_wrap_positive(self):
        """
            Perform a positive wrap on geospatial data
        """
        self.geo_object.wrap({"type": "Polygon", "coordinates": [[[-122.500000, 
            37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
            38.080000], [-122.500000, 37.000000]]]})
        assert self.geo_object.unwrap() == {'coordinates': [[[-122.5, 37.0], [-121.0, 37.0], [-121.0, 38.08],
            [-122.5, 38.08], [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_object_wrap_non_dict(self):
        """
            The geospatial object provided to wrap() is not a dictionary
        """
        try:
            self.geo_object.wrap("abc")
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == 'Geospatial data should be a dictionary'

    def test_geospatial_wrap_positive_with_query(self):
        """
            Perform a positive wrap on geospatial data followed by a query
        """
        geo_object_wrap = aerospike.GeoJSON({"type": "Polygon", "coordinates": [[[-124.500000,
    37.000000],[-125.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
        38.080000], [-124.500000, 37.000000]]]})

        geo_object_wrap.wrap({"type": "Polygon", "coordinates": [[[-122.500000, 
            37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
            38.080000], [-122.500000, 37.000000]]]})
        assert geo_object_wrap.unwrap() == {'coordinates': [[[-122.5, 37.0], [-121.0, 37.0], [-121.0, 38.08],
            [-122.5, 38.08], [-122.5, 37.0]]], 'type': 'Polygon'}
        
	records = []
        query = TestGeospatial.client.query("test", "demo")
        query.where(p.within("loc", geo_object_wrap.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        assert records == [{'loc': {'coordinates': [-122.0, 37.5], 'type': 'Point'}}, {'loc': {'coordinates': [-121.8, 37.7], 'type':
                'Point'}}, {'loc': {'coordinates': [-121.6, 37.9], 'type': 'Point'}}]

    def test_geospatial_loads_positive(self):
        """
            Perform a positive loads on geoJSON raw string
        """
        self.geo_object.loads('{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000], [-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert self.geo_object.unwrap() == {'coordinates': [[[-122.5, 37.0], [-121.0, 37.0], [-121.0, 38.08],
            [-122.5, 38.08], [-122.5, 37.0]]], 'type': 'Polygon'}

    def test_geospatial_object_loads_non_dict(self):
        """
            The geospatial object provided to loads() is not a dictionary
        """
        try:
            self.geo_object.loads('{"abc"}')
        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == 'String is not geoJSON serializable'

    def test_geospatial_loads_positive_with_query(self):
        """
            Perform a positive loads on geoJSON raw string followed by a query
        """
        geo_object_loads = aerospike.GeoJSON({"type": "Polygon", "coordinates": [[[-124.500000,
    37.000000],[-125.000000, 37.000000], [-121.000000, 38.080000],[-122.500000,
        38.080000], [-124.500000, 37.000000]]]})

        geo_object_loads.loads('{"type": "Polygon", "coordinates": [[[-122.500000, 37.000000], [-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]]]}')

        assert geo_object_loads.unwrap() == {'coordinates': [[[-122.5, 37.0], [-121.0, 37.0], [-121.0, 38.08],
            [-122.5, 38.08], [-122.5, 37.0]]], 'type': 'Polygon'}

	records = []
        query = TestGeospatial.client.query("test", "demo")
        query.where(p.within("loc", geo_object_loads.dumps()))

        def callback((key, metadata, record)):
            records.append(record)

        query.foreach(callback)

        assert len(records) == 3
        assert records == [{'loc': {'coordinates': [-122.0, 37.5], 'type': 'Point'}}, {'loc': {'coordinates': [-121.8, 37.7], 'type':
                'Point'}}, {'loc': {'coordinates': [-121.6, 37.9], 'type': 'Point'}}]

    def test_geospatial_dumps_positive(self):
        """
            Perform a positive dumps. Verify using str
        """
        assert self.geo_object.dumps() == '{"type": "Point", "coordinates": [-120.2, 39.3]}'

        assert str(self.geo_object) == '{"type": "Point", "coordinates": [-120.2, 39.3]}'

    def test_geospatial_repr_positive(self):
        """
            Perform a positive repr. Verify using eval()
        """
	assert repr(self.geo_object) == '\'{"type": "Point", "coordinates": [-120.2, 39.3]}\''
        assert eval(repr(self.geo_object)) == '{"type": "Point", "coordinates": [-120.2, 39.3]}'

    def test_geospatial_2dindex_positive(self):
        """
            Perform a positive 2d index creation
        """
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0

        status = TestGeospatial.client.index_geo2dsphere_create("test", "demo", "loc", "loc_index")

        assert status == 0

    def test_geospatial_2dindex_positive_with_policy(self):
        """
            Perform a positive 2d index creation with policy
        """
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0

        status = TestGeospatial.client.index_geo2dsphere_create("test", "demo", "loc", "loc_index", {"timeout": 2000})

        assert status == 0

    def test_geospatial_2dindex_set_length_extra(self):
        """
            Perform a 2d creation with set length exceeding limit
        """
        set_name = 'a'
        for i in xrange(100):
            set_name = set_name + 'a'
        status = TestGeospatial.client.index_remove('test', 'loc_index')

        assert status == 0
	try:
        	status = TestGeospatial.client.index_geo2dsphere_create("test", set_name, "loc", "loc_index")

	except InvalidRequest as exception:
        	assert exception.code == 4
		assert exception.msg == "Invalid Set Name"
