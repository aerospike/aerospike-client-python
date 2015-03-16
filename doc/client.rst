.. _client:


.. currentmodule:: aerospike


=================================
Client Class --- :class:`Client`
=================================

:class:`Client`
===============

.. class:: Client

    Example::

        # import the module
        from __future__ import print_function
        import aerospike

        # Configure the client
        config = {
            'hosts': [ ('127.0.0.1', 3000) ]
        }

        # Create a client and connect it to the cluster
        try:
            client = aerospike.client(config).connect()
        except:
            print("failed to connect to the cluster with". config['hosts'])
            sys.exit(1)

        # Records are addressable via a tuple of (namespace, set, key)
        key = ('test', 'demo', 'foo')

        try:
            # Write a record
            client.put(key, {
                'name': 'John Doe',
                'age': 32
            })
        except Exception as e:
            print("error: {0}".format(e), file=sys.stderr)

        # Read a record
        (key, metadata, record) = client.get(key)

        # Close the connection to the Aerospike cluster
        client.close()


    .. seealso::
        `Client Architecture
        <https://www.aerospike.com/docs/architecture/clients.html>`_.


    .. method:: connect()

        Connect to the cluster.

    .. method:: close()

        Close all connections to the cluster.


    .. method:: get(key[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :class:`tuple` consisting of *key*, *meta* and *bins*.  If the record \
        does not exist the *meta* data will be ``None``.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional read policies :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta, bins) = client.get(key)

                print(key)
                print('--------------------------')
                print(meta)
                print('--------------------------')
                print(bins)
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: put(key, bins[, meta[, policy]])

        Write a record with a given *key* to the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param dict bins: a :class:`dict` of bin-name / bin-value pairs.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional read policies :ref:`aerospike_write_policies`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            config = {
                'hosts': [ ('127.0.0.1', 3000) ],
                'timeout': 1500
            }
            client = aerospike.client(config).connect()
            try:
                key = ('test', 'demo', 1)
                bins = {
                    'l': [ "qwertyuiop", 1, bytearray("asd;as[d'as;d", "utf-8") ],
                    'm': { "key": "asd';q;'1';" },
                    'i': 1234,
                    's': '!@#@#$QSDAsd;as'
                }
                client.put(key, bins,
                         policy={'key': aerospike.POLICY_KEY_SEND},
                         meta={'ttl':180})
                # adding a bin
                client.put(key, {'smiley': u"\ud83d\ude04"})

            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: remove(key[, policy])

        Remove a record matching the *key* from the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional read policies :ref:`aerospike_remove_policies`.

        .. code-block:: python

            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            key = ('test', 'demo', 1)
            client.remove(key, {'retry': aerospike.POLICY_RETRY_ONCE})


    .. method:: append(key, bin, val[, meta[, policy]])

        Append the string *val* to the string value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to append to the value of *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional operate policies :ref:`aerospike_operate_policies`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.append(key, 'name', ' jr.', policy={'timeout': 1200})
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: prepend(key, bin, val[, meta[, policy]])

        Append the string *val* to the string value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to prepend to the value of *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional operate policies :ref:`aerospike_operate_policies`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.prepend(key, 'name', 'Dr. ', policy={'timeout': 1200})
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: increment(key, bin, offset[, meta[, policy]])

        Append the string *val* to the string value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int offset: the integer by which to increment the value in the *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional operate policies :ref:`aerospike_operate_policies`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                client.put(('test', 'cats', 'mr. peppy'), {'breed':'persian'}, policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE})
                (key, meta, bins) = client.get(('test', 'cats', 'mr. peppy'))
                print("Before:", bins, "\n")
                client.increment(key, 'lives', -1)
                (key, meta, bins) = client.get(key)
                print("After:", bins, "\n")
                client.increment(key, 'lives', -1)
                (key, meta, bins) = client.get(key)
                print("Poor Kitty:", bins, "\n")
                print(bins)
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


.. _aerospike_key_tuple:

Key Tuple
---------

.. object:: key

    The key tuple which is sent and returned by various operations contains

    ``(namespace, set, primary key, the record's RIPEMD-160 digest)``


.. _aerospike_record_tuple:

Record Tuple
------------

.. object:: record

    The record tuple ``(key, meta, bins)`` which is returned by various read operations.

    .. hlist::
        :columns: 1

        * *key* the tuple ``(namespace, set, primary key, the record's RIPEMD-160 digest)``
        * *meta* a dict containing  {'gen' : genration value, 'ttl': ttl value}
        * *bins* a dict containing bin-name/bin-value pairs


.. _aerospike_write_policies:

Write Policies
--------------

.. object:: policy

    Optional write policies applicable to :meth:`put`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **exists** one of the `aerospike_POLICY_EXISTS_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga50b94613bcf416c9c2691c9831b89238>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values


.. _aerospike_read_policies:

Read Policies
-------------

.. object:: policy

    Optional read policies applicable to :meth:`get`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **consistency_level** one of the `aerospike.POLICY_CONSISTENCY_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga34dbe8d01c941be845145af643f9b5ab>`_ values
        * **replica** one of the `aerospike_POLICY_REPLICA_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gabce1fb468ee9cbfe54b7ab834cec79ab>`_ values



.. _aerospike_operate_policies:

Operate Policies
----------------

.. object:: policy

    Optional operate policies applicable to :meth:`append`, :meth:`prepend`, \
    :meth:`increment`, :meth:`operate`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values
        * **replica** one of the `aerospike_POLICY_REPLICA_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gabce1fb468ee9cbfe54b7ab834cec79ab>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values
        * **consistency_level** one of the `aerospike.POLICY_CONSISTENCY_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga34dbe8d01c941be845145af643f9b5ab>`_ values


.. _aerospike_remove_policies:

Remove Policies
---------------

.. object:: policy

    Optional remove policies applicable to :meth:`remove`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values


