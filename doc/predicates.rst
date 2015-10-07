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
    :return: :py:func:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        from __future__ import print_function
        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        query = client.query('test', 'demo')
        query.where(p.between('age', 20, 30))
        res = query.results()
        print(res)
        client.close


.. py:function:: equals(bin, val)

    Represent a *bin* **=** *val* predicate.

    :param str bin: the bin name.
    :param val: the value to be matched with an equals operator.
    :type val: :py:class:`str` or :py:class:`int`
    :return: :py:func:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        from __future__ import print_function
        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        query = client.query('test', 'demo')
        query.where(p.equal('name', 'that guy'))
        res = query.results()
        print(res)
        client.close

.. py:function:: contains(bin, index_type, val)

    Represent the predicate *bin* **CONTAINS** *val* for a bin with a complex \
    (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param val: match records whose *bin* is an *index_type* (ex: list) containing *val*.
    :type val: :py:class:`str` or :py:class:`int`
    :return: :py:func:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. warning::

        This functionality will become available with a future release of the Aerospike server.

    .. code-block:: python

        from __future__ import print_function
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
        client.close

.. py:function:: range(bin, index_type, min, max))

    Represent the predicate *bin* **CONTAINS** values **BETWEEN** *min* **AND** \
    *max* for a bin with a complex (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param int min: the minimum value to be used for matching with the range operator.
    :param int max: the maximum value to be used for matching with the range operator.
    :return: :py:func:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. warning::

        This functionality will become available with a future release of the Aerospike server.

    .. code-block:: python

        from __future__ import print_function
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
        client.close

.. py:function:: geo_within(bin, shape)

    Predicate for finding any point in bin which is within the given shape.
    Requires a geo2dsphere index
    (:meth:`~aerospike.Client.index_geo2dsphere_create`) over a *bin*
    containing :class:`~aerospike.GeoJSON` point data.

    :param str bin: the bin name.
    :param str shape: find points that are within the shape described by a GeoJSON string.
    :return: :py:func:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        from __future__ import print_function
        import aerospike
        from aerospike import GeoJSON
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        # Create a search rectangle which matches screen boundaries:
        rect = GeoJSON({ 'type': "Polygon",
                         'coordinates': [
                          [[28.60000, -80.590000],
                           [28.61800, -80.590000],
                           [28.61800, -80.620000],
                           [28.600000,-80.620000]]]})

        # Find all points contained in the rectangle.
        query = client.query('test', 'demo')
        query.select('userid', 'tstamp', 'loc')
        query.where(p.geo_within('loc', rect.dumps()))
        points = query.results()
        print(points)
        client.close

