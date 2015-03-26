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
    :param min: the minimum value to be matched with the between operator.
    :type min: str or int
    :param max: the maximum value to be matched with the between operator.
    :type max: str or int
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

.. py:function:: contains(bin, index_type, index_datatype, val)

    Represent a *bin* **=** *val* predicate.

    :param str bin: the bin name.
    :param str index_type: The index type. Possible values are "LIST", "MAPVALUES", "MAPKEYS"
    :param index_datatype: Possible values are aerospike.INDEX_STRING and aerospike.INDEX_NUMERIC
    :param val: the value to be matched with an equals operator.
    :type val: str or int
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)
        query = self.client.query('test', 'demo')
        query.where(p.contains('name', "LIST", aerospike.INDEX_STRING, 'that guy'))

.. py:function:: range_contains(bin, index_type, index_datatype, min, max))

    Represent a *bin* **=** *val* predicate.

    :param str bin: the bin name.
    :param str index_type: The index type. Possible values are "LIST", "MAPVALUES", "MAPKEYS"
    :param index_datatype: Possible values are aerospike.INDEX_NUMERIC
    :param min: the minimum value to be matched with the range_contains operator.
    :type min: int
    :param max: the maximum value to be matched with the range_contains operator.
    :type max: int
    :return: :class:`tuple` to be used in :meth:`aerospike.Query.where`.

    .. code-block:: python

        import aerospike
        from aerospike import predicates as p

        config = { 'hosts': [ ('127.0.0.1', 3000)]}
        client = aerospike.client(config)
        query = self.client.query('test', 'demo')
        query.where(p.range_contains('age', "LIST", aerospike.INDEX_NUMERIC, 1, 3))
