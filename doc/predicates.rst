.. _aerospike.predicates:

*************************************************
:mod:`aerospike.predicates` --- Query Predicates
*************************************************

.. module:: aerospike.predicates
    :platform: 64-bit Linux and OS X
    :synopsis: Query predicate helper functions.


.. py:function:: between(bin, min, max)

    Represent a *bin* **BETWEEN** *min* **AND** *max* predicate.

    :param str bin: the bin name.
    :param int min: the minimum value to be matched with the between operator.
    :param int max: the maximum value to be matched with the between operator.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        query = client.query('test', 'demo')
        query.where(p.between('age', 20, 30))
        res = query.results()
        print(res)
        client.close()


.. py:function:: equals(bin, val)

    Represent a *bin* **=** *val* predicate.

    :param str bin: the bin name.
    :param val: the value to be matched with an equals operator.
    :type val: :py:class:`str` or :py:class:`int`
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        query = client.query('test', 'demo')
        query.where(p.equals('name', 'that guy'))
        res = query.results()
        print(res)
        client.close()

.. py:function:: geo_within_geojson_region(bin, shape[, index_type])

    Predicate for finding any point in bin which is within the given shape.
    Requires a geo2dsphere index
    (:meth:`~aerospike.index_geo2dsphere_create`) over a *bin*
    containing :class:`~aerospike.GeoJSON` point data.

    :param str bin: the bin name.
    :param str shape: the shape formatted as a GeoJSON string.
    :param index_type: Optional. Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.7.0

    .. code-block:: python

        import aerospike
        from aerospike import GeoJSON
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')
        bins = {'pad_id': 1,
                'loc': aerospike.geojson('{"type":"Point", "coordinates":[-80.604333, 28.608389]}')}
        client.put(('test', 'pads', 'launchpad1'), bins)

        # Create a search rectangle which matches screen boundaries:
        # (from the bottom left corner counter-clockwise)
        scrn = GeoJSON({ 'type': "Polygon",
                         'coordinates': [
                          [[-80.590000, 28.60000],
                           [-80.590000, 28.61800],
                           [-80.620000, 28.61800],
                           [-80.620000, 28.60000],
                           [-80.590000, 28.60000]]]})

        # Find all points contained in the rectangle.
        query = client.query('test', 'pads')
        query.select('pad_id', 'loc')
        query.where(p.geo_within_geojson_region('loc', scrn.dumps()))
        records = query.results()
        print(records)
        client.close()

    .. versionadded:: 1.0.58

.. py:function:: geo_within_radius(bin, long, lat, radius_meters[, index_type])

    Predicate helper builds an AeroCircle GeoJSON shape, and returns a
    'within GeoJSON region' predicate.
    Requires a geo2dsphere index
    (:meth:`~aerospike.index_geo2dsphere_create`) over a *bin*
    containing :class:`~aerospike.GeoJSON` point data.

    :param str bin: the bin name.
    :param float long: the longitude of the center point of the AeroCircle.
    :param float lat: the latitude of the center point of the AeroCircle.
    :param float radius_meters: the radius length in meters of the AeroCircle.
    :param index_type: Optional. Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.8.1

    .. code-block:: python

        import aerospike
        from aerospike import GeoJSON
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')
        bins = {'pad_id': 1,
                'loc': aerospike.geojson('{"type":"Point", "coordinates":[-80.604333, 28.608389]}')}
        client.put(('test', 'pads', 'launchpad1'), bins)

        query = client.query('test', 'pads')
        query.select('pad_id', 'loc')
        query.where(p.geo_within_radius('loc', -80.605000, 28.60900, 400.0))
        records = query.results()
        print(records)
        client.close()

    .. versionadded:: 1.0.58

.. py:function:: geo_contains_geojson_point(bin, point[, index_type])

    Predicate for finding any regions in the bin which contain the given point.
    Requires a geo2dsphere index
    (:meth:`~aerospike.index_geo2dsphere_create`) over a *bin*
    containing :class:`~aerospike.GeoJSON` point data.

    :param str bin: the bin name.
    :param str point: the point formatted as a GeoJSON string.
    :param index_type: Optional. Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.7.0

    .. code-block:: python

        import aerospike
        from aerospike import GeoJSON
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        client.index_geo2dsphere_create('test', 'launch_centers', 'area', 'launch_area_geo')
        rect = GeoJSON({ 'type': "Polygon",
                         'coordinates': [
                          [[-80.590000, 28.60000],
                           [-80.590000, 28.61800],
                           [-80.620000, 28.61800],
                           [-80.620000, 28.60000],
                           [-80.590000, 28.60000]]]})
        bins = {'area': rect}
        client.put(('test', 'launch_centers', 'kennedy space center'), bins)

        # Find all geo regions containing a point
        point = GeoJSON({'type': "Point",
                         'coordinates': [-80.604333, 28.608389]})
        query = client.query('test', 'launch_centers')
        query.where(p.geo_contains_geojson_point('area', point.dumps()))
        records = query.results()
        print(records)
        client.close()

    .. versionadded:: 1.0.58

.. py:function:: geo_contains_point(bin, long, lat[, index_type])

    Predicate helper builds a GeoJSON point, and returns a
    'contains GeoJSON point' predicate.
    Requires a geo2dsphere index
    (:meth:`~aerospike.index_geo2dsphere_create`) over a *bin*
    containing :class:`~aerospike.GeoJSON` point data.

    :param str bin: the bin name.
    :param float long: the longitude of the point.
    :param float lat: the latitude of the point.
    :param index_type: Optional. Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.7.0

    .. code-block:: python

        import aerospike
        from aerospike import GeoJSON
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        client.index_geo2dsphere_create('test', 'launch_centers', 'area', 'launch_area_geo')
        rect = GeoJSON({ 'type': "Polygon",
                         'coordinates': [
                          [[-80.590000, 28.60000],
                           [-80.590000, 28.61800],
                           [-80.620000, 28.61800],
                           [-80.620000, 28.60000],
                           [-80.590000, 28.60000]]]})
        bins = {'area': rect}
        client.put(('test', 'launch_centers', 'kennedy space center'), bins)

        # Find all geo regions containing a point
        query = client.query('test', 'launch_centers')
        query.where(p.geo_contains_point('area', -80.604333, 28.608389))
        records = query.results()
        print(records)
        client.close()

    .. versionadded:: 1.0.58

.. py:function:: contains(bin, index_type, val)

    Represent the predicate *bin* **CONTAINS** *val* for a bin with a complex \
    (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param val: match records whose *bin* is an *index_type* (ex: list) containing *val*.
    :type val: :py:class:`str` or :py:class:`int`
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.8.1

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        # assume the bin fav_movies in the set test.demo bin should contain
        # a dict { (str) _title_ : (int) _times_viewed_ }
        # create a secondary index for string values of test.demo records whose 'fav_movies' bin is a map
        client.index_map_keys_create('test', 'demo', 'fav_movies', aerospike.INDEX_STRING, 'demo_fav_movies_titles_idx')
        # create a secondary index for integer values of test.demo records whose 'fav_movies' bin is a map
        client.index_map_values_create('test', 'demo', 'fav_movies', aerospike.INDEX_NUMERIC, 'demo_fav_movies_views_idx')

        client.put(('test','demo','Dr. Doom'), {'age':43, 'fav_movies': {'12 Monkeys': 1, 'Brasil': 2}})
        client.put(('test','demo','The Hulk'), {'age':38, 'fav_movies': {'Blindness': 1, 'Eternal Sunshine': 2}})

        query = client.query('test', 'demo')
        query.where(p.contains('fav_movies', aerospike.INDEX_TYPE_MAPKEYS, '12 Monkeys'))
        res = query.results()
        print(res)
        client.close()

.. py:function:: range(bin, index_type, min, max))

    Represent the predicate *bin* **CONTAINS** values **BETWEEN** *min* **AND** \
    *max* for a bin with a complex (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param int min: the minimum value to be used for matching with the range operator.
    :param int max: the maximum value to be used for matching with the range operator.
    :return: `tuple` to be used in :meth:`aerospike.Query.where`.

    .. note:: Requires server version >= 3.8.1

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()

        # create a secondary index for numeric values of test.demo records whose 'age' bin is a list
        client.index_list_create('test', 'demo', 'age', aerospike.INDEX_NUMERIC, 'demo_age_nidx')

        # query for records whose 'age' bin has a list with numeric values between 20 and 30
        query = client.query('test', 'demo')
        query.where(p.range('age', aerospike.INDEX_TYPE_LIST, 20, 30))
        res = query.results()
        print(res)
        client.close()

