.. _geojson:

.. currentmodule:: aerospike

==================================
GeoJSON Class --- :class:`GeoJSON`
==================================

:class:`GeoJSON`
================

.. class:: GeoJSON

    A near future version of the Aerospike server will support storing
    GeoJSON data. A geo2dsphere index can be built on a bin which contains
    GeoJSON data, enabling queries for the points contained within given
    shapes using :meth:`aerospike.predicates.geo_within`.

    On the client side, wrapping geospatial data in an instance of the
    :class:`aerospike.GeoJSON` class enables serialization of the data into the
    correct type during write operation, such as :meth:`~aerospike.Client.put`.
    On reading a record from the server, bins with geospatial data it will be
    deserialized into a :class:`~aerospike.GeoJSON` instance.

    .. code-block:: python

        from __future__ import print_function
        import aerospike
        from aerospike import GeoJSON
        import time

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        client.index_geo2dsphere_create('test', 'demo', 'loc', 'loc_geo_idx')
        # Create GeoJSON point using WGS84 coordinates.
        latitude = 28.608389
        longitude = -80.604333
        loc = GeoJSON({'type': "Point",
                       'coordinates': [longitude, latitude]})
        print(loc)
        # Alternatively create the GeoJSON point from a string
        loc = GeoJSON('{"type": "Point", "coordinates": [28.608389, -80.604333]}')

        # Create a user record.
        bins = {'tstamp': time.time(),
                'userid': 12345,
                'loc': loc}

        # Store the record.
        client.put(('test', 'demo', 'geo1'), bins)

        # Read the record.
        (k, m, b) = client.get(('test', 'demo', 'geo1'))
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

