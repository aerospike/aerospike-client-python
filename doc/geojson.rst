.. _aerospike.geojson:

.. currentmodule:: aerospike

==================================
GeoJSON Class --- :class:`GeoJSON`
==================================

:class:`GeoJSON`
================

.. class:: GeoJSON

    Starting with version 3.7.0, the Aerospike server supports storing
    GeoJSON data. A Geo2DSphere index can be built on a bin which contains
    GeoJSON data, enabling queries for the points contained within given
    shapes using :meth:`~aerospike.predicates.geo_within_geojson_region` and
    :meth:`~aerospike.predicates.geo_within_radius`, and for the regions which
    contain a point using :meth:`~aerospike.predicates.geo_contains_geojson_point`
    and :meth:`~aerospike.predicates.geo_contains_point`.

    On the client side, wrapping geospatial data in an instance of the
    :class:`aerospike.GeoJSON` class enables serialization of the data into the
    correct type during write operation, such as :meth:`~aerospike.put`.
    On reading a record from the server, bins with geospatial data it will be
    deserialized into a :class:`~aerospike.GeoJSON` instance.

    .. seealso::
        `Geospatial Index and Query
        <http://www.aerospike.com/docs/guide/geospatial.html>`_.

    .. code-block:: python

        import aerospike
        from aerospike import GeoJSON

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')
        # Create GeoJSON point using WGS84 coordinates.
        latitude = 28.608389
        longitude = -80.604333
        loc = GeoJSON({'type': "Point",
                       'coordinates': [longitude, latitude]})
        print(loc)
        # Alternatively create the GeoJSON point from a string
        loc = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')

        # Create a user record.
        bins = {'pad_id': 1,
                'loc': loc}

        # Store the record.
        client.put(('test', 'pads', 'launchpad1'), bins)

        # Read the record.
        (k, m, b) = client.get(('test', 'pads', 'launchpad1'))
        print(b)
        client.close()


    .. class:: GeoJSON([geo_data])

        Optionally initializes an object with a GeoJSON :class:`str` or a :class:`dict` of geospatial data.

    .. method:: wrap(geo_data)

        Sets the geospatial data of the :class:`~aerospike.GeoJSON` wrapper class.

        :param dict geo_data: a :class:`dict` representing the geospatial data.

    .. method:: unwrap() -> dict of geospatial data

        Gets the geospatial data contained in the :class:`~aerospike.GeoJSON` class.

        :return: a :class:`dict` representing the geospatial data.

    .. method:: loads(raw_geo)

        Sets the geospatial data of the :class:`~aerospike.GeoJSON` wrapper class from a GeoJSON string.

        :param str raw_geo: a GeoJSON string representation.

    .. method:: dumps() -> a GeoJSON string

        Gets the geospatial data contained in the :class:`~aerospike.GeoJSON` class as a GeoJSON string.

        :return: a GeoJSON :class:`str` representing the geospatial data.


.. versionadded:: 1.0.53

