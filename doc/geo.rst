.. _geo:

.. currentmodule:: aerospike

=================================
Geo Class --- :class:`Geo`
=================================

:class:`Geo`
===============

.. class:: Geo

    Starting with version 3.6.2, the Aerospike server supports storing
    geospatial data. A 2dsphere index can be built on a bin which contains
    geospatial data, enabling queries for the points contained within given
    shapes using :meth:`aerospike.predicates.within`.

    On the client side, wrapping geospatial data in an instance of the
    :class:`aerospike.Geo` class enables serialization of the data into the
    correct type during write operation, such as :meth:`~aerospike.Client.put`.
    On reading a record from the server, bins with geospatial data it will be
    deserialized into a :class:`~aerospike.Geo` instance.

    .. code-block:: python

        from __future__ import print_function
        import aerospike
        import time

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        # Create GeoJSON point using WGS84 coordinates.
        latitude = 28.608389
        longitude = -80.604333
        loc = aerospike.geo({'type': "Point",
                             'coordinates': [longitude, latitude] })
        print(loc)

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


    .. method:: wrap(geo_data)

        Sets the geospatial data of the :class:`~aerospike.Geo` wrapper class.

        :param dict geo_data: a :class:`dict` representing the geospatial data.

    .. method:: unwrap() -> dict of geospatial data

        Gets the geospatial data contained in the :class:`~aerospike.Geo` class.

        :return: a :class:`dict` representing the geospatial data.

    .. method:: loads(raw_geo)

        Sets the geospatial data of the :class:`~aerospike.Geo` wrapper class from a raw GeoJSON string.

        :param str raw_geo: a GeoJSON string representation.

    .. method:: dumps() -> a GeoJSON string

        Gets the geospatial data contained in the :class:`~aerospike.Geo` class as a GeoJSON string.

        :return: a GeoJSON :class:`str` representing the geospatial data.


.. versionadded:: 1.0.53

