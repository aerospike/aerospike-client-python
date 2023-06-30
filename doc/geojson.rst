.. _aerospike.geojson:

.. currentmodule:: aerospike

============================================
:class:`aerospike.GeoJSON` --- GeoJSON Class
============================================

Overview
========

Starting with version ``3.7.0``, the Aerospike server supports storing GeoJSON data.
A Geo2DSphere index can be built on a bin which contains GeoJSON data,
which allows queries for points inside any given shapes using:

* :meth:`~aerospike.predicates.geo_within_geojson_region`
* :meth:`~aerospike.predicates.geo_within_radius`

It also enables queries for regions that contain a given point using:

* :meth:`~aerospike.predicates.geo_contains_geojson_point`
* :meth:`~aerospike.predicates.geo_contains_point`

On the client side, wrapping geospatial data in an instance of the
:class:`aerospike.GeoJSON` class enables serialization of the data into the
correct type during a write operation, such as :meth:`~aerospike.Client.put`.

When reading a record from the server, bins with geospatial data will be
deserialized into a :class:`~aerospike.GeoJSON` instance.

.. seealso::
    `Geospatial Index and Query
    <https://docs.aerospike.com/server/guide/data-types/geospatial>`_.

.. _example:

Example
-------

.. code-block:: python

    import aerospike
    from aerospike import GeoJSON

    config = { 'hosts': [ ('127.0.0.1', 3000)]}
    client = aerospike.client(config)

    client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')

    # Create GeoJSON point using WGS84 coordinates.
    latitude = 28.608389
    longitude = -80.604333
    loc = GeoJSON({'type': "Point",
                    'coordinates': [longitude, latitude]})
    print(loc)

    # Expected output:
    # {"type": "Point", "coordinates": [-80.604333, 28.608389]}

    # Alternatively, create the GeoJSON point from a string
    loc = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')

    # Create and store a user record.
    bins = {'pad_id': 1,
            'loc': loc}
    client.put(('test', 'pads', 'launchpad1'), bins)

    # Read the record.
    (k, m, b) = client.get(('test', 'pads', 'launchpad1'))
    print(b)

    # Expected output:
    # {'pad_id': 1, 'loc': '{"type": "Point", "coordinates": [-80.604333, 28.608389]}'}

    # Cleanup
    client.remove(('test', 'pads', 'launchpad1'))
    client.close()

Methods
=======

.. class:: GeoJSON

    .. class:: GeoJSON([geo_data])

        Optionally initializes an object with a GeoJSON :class:`str` or a :class:`dict` of geospatial data.

        See :ref:`example` for usage.

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
