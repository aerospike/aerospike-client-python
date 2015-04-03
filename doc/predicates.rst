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
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)
        query = self.client.query('test', 'demo')
        query.where(p.between('age', 20, 30))


.. py:function:: equals(bin, val)

    Represent a *bin* **=** *val* predicate.

    :param str bin: the bin name.
    :param val: the value to be matched with an equals operator.
    :type val: str or int
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)
        query = self.client.query('test', 'demo')
        query.where(p.equal('name', 'that guy'))

.. py:function:: contains(bin, index_type, val)

    Represent the predicate *bin* **CONTAINS** *val* for a bin with a complex \
    (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param val: match records whose *bin* is an *index_type* (ex: list) containing *val*.
    :type val: str or int
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. warning::

        This functionality will become available with a future release of the Aerospike server.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)

        # assume the bin fav_movies in the set test.demo bin should contain
        # a dict { (str) _title_ : (int) _times_viewed_ }
        # create a secondary index for string values of test.demo records whose 'fav_movies' bin is a map
        client.index_map_keys_create('test', 'demo', 'fav_movies', aerospike.INDEX_STRING, 'demo_fav_movies_titles_idx')
        # create a secondary index for integer values of test.demo records whose 'fav_movies' bin is a map
        client.index_map_values_create('test', 'demo', 'fav_movies', aerospike.INDEX_NUMERIC, 'demo_fav_movies_views_idx')

        query = self.client.query('test', 'demo')
        query.where(p.contains('fav_movies', aerospike.INDEX_TYPE_MAPKEYS, '12 Monkeys'))

.. py:function:: range(bin, index_type, min, max))

    Represent the predicate *bin* **CONTAINS** values **BETWEEN** *min* **AND** \
    *max* for a bin with a complex (list or map) type.

    :param str bin: the bin name.
    :param index_type: Possible ``aerospike.INDEX_TYPE_*`` values are detailed in :ref:`aerospike_misc_constants`.
    :param int min: the minimum value to be used for matching with the range operator.
    :param int max: the maximum value to be used for matching with the range operator.
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. warning::

        This functionality will become available with a future release of the Aerospike server.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)

        # create a secondary index for numeric values of test.demo records whose 'age' bin is a list
        client.index_list_create('test', 'demo', 'age', aerospike.INDEX_NUMERIC, 'demo_age_nidx')

        # query for records whose 'age' bin has a list with numeric values between 20 and 30
        query = self.client.query('test', 'demo')
        query.where(p.range('age', aerospike.INDEX_TYPE_LIST, 20, 30))
