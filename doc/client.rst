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
        import sys

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
        (key, meta, record) = client.get(key)

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
            import sys

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
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: select(key, bins[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :class:`tuple` consisting of *key*, *meta* and *bins*, with the \
        specified bins preojected. If the record does not exist the *meta* \
        data will be ``None``. If a selected bin does not exist its value will \
        be ``None``.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list bins: a list of bin names to select from the record.
        :param dict policy: optional read policies :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta, bins) = client.select(key, ['name'])

                print(key)
                print('--------------------------')
                print(meta)
                print('--------------------------')
                print(bins)
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. method:: exists(key[, policy]) -> (key, meta)

        Check if a record with a given *key* exists in the cluster and return \
        the record as a :class:`tuple` consisting of *key* and *meta*.  If \
        the record  does not exist the *meta* data will be ``None``.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional read policies :ref:`aerospike_read_policies`.
        :rtype: :class:`tuple` (key, meta)

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta) = client.exists(key)

                print(key)
                print('--------------------------')
                print(meta)
                client.close()
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
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)

        .. note:: Using Generation Policy

            The generation policy allows a record to be written only when the \
            generation is a specific value. In the following example, we only \
            want to write the record if no change has occured since \
            :meth:`exists` was called.

            .. code-block:: python

                from __future__ import print_function
                import aerospike
                import sys

                config = { 'hosts': [ ('127.0.0.1',3000)]}
                client = aerospike.client(config).connect()

                try:
                    (key, meta) = client.exists(('test','test','key1'))
                    print(meta)
                    print('============')
                    client.put(('test','test','key1'), {'id':1,'a':2},
                        policy={'gen':aerospike.POLICY_GEN_EQ},
                        meta={'gen': 33})
                    print('Record written.')
                except Exception as e:
                    print("error: {0}".format(e), file=sys.stderr)

    .. method:: touch(key[, val=0[, meta[, policy]]])

        Touch the given record, resetting its \
        `time-to-live <http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl>`_ \
        and incrementing its generation.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param int val: the optional ttl in seconds, with ``0`` resolving to the default value in the server config.
        :param dict meta: optional record metadata to be set.
        :param dict policy: optional operate policies :ref:`aerospike_operate_policies`.

        .. seealso:: `Record TTL and Evictions <https://discuss.aerospike.com/t/records-ttl-and-evictions/737>`_ \
                     and `FAQ <https://www.aerospike.com/docs/guide/FAQ.html>`_.

        .. code-block:: python

            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            key = ('test', 'demo', 1)
            client.touch(key, 120, policy={'timeout': 100})
            client.close()


    .. method:: remove(key[, policy])

        Remove a record matching the *key* from the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional remove policies :ref:`aerospike_remove_policies`.

        .. code-block:: python

            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            key = ('test', 'demo', 1)
            client.remove(key, {'retry': aerospike.POLICY_RETRY_ONCE
            client.close()


    .. method:: get_key_digest(ns, set, key) -> bytearray

        Calculate the digest of a particular key. See: :ref:`aerospike_key_tuple`.

        :param str ns: the namespace in the aerospike database.
        :param str set: the set name.
        :param key: the primary key identifier of the record within the set.
        :type key: str or int
        :return: a RIPEMD-160 digest of the input tuple.
        :rtype: bytearray

        .. code-block:: python

            import aerospike
            import pprint

            pp = pprint.PrettyPrinter(indent=2)
            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            digest = client.get_key_digest("test", "demo", 1 )
            pp.pprint(digest)
            key = ('test', 'demo', None, digest)
            (key, meta, bins) = client.get(key)
            pp.pprint(bins)
            client.close()


    .. rubric:: Bin Operations


    .. method:: remove_bin(key, list[, meta[, policy]])

        Remove a list of bins from a record with a given *key*.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: the bins names to be removed from the record.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional write policies :ref:`aerospike_write_policies`.

        .. code-block:: python

            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            key = ('test', 'demo', 1)
            meta = { 'ttl': 3600 }
            client.remove_bin(key, ['name', 'age'], meta, {'retry': aerospike.POLICY_RETRY_ONCE})
            client.close()


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
            import sys

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
            import sys

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
            import sys

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


    .. method:: operate(key, list[, meta[, policy]]) -> (key, meta, bins)

        Perform multiple bin operations on a record with a given *key*, \
        with write operations happening before read ops. Non-existent bins \
        being read will have a ``None`` value.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: a :class:`list` of one or more bin operations, each \
            structured as the :class:`dict` \
            ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
            See :ref:`aerospike_operators`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional operate policies :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.put(key, {'count': 1})
                list = [
                    {
                      "op" : aerospike.OPERATOR_INCR,
                      "bin": "count",
                      "val": 2
                    },
                    {
                      "op" : aerospike.OPERATOR_PREPEND,
                      "bin": "name",
                      "val": ":start:"
                    },
                    {
                      "op" : aerospike.OPERATOR_APPEND,
                      "bin": "name",
                      "val": ":end:"
                    },
                    {
                      "op" : aerospike.OPERATOR_READ,
                      "bin": "name"
                    },
                    {
                      "op" : aerospike.OPERATOR_WRITE,
                      "bin": "age",
                      "val": 39
                    },
                    {
                      "op" : aerospike.OPERATOR_TOUCH,
                      "val": 360
                    }
                ]
                (key, meta, bins) = self.client.operate(key, list, policy={'timeout':500})

                print(key)
                print('--------------------------')
                print(meta)
                print('--------------------------')
                print(bins)
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)


    .. rubric:: Batch Operations

    .. method:: get_many(keys[, policy]) -> {primary_key: (key. meta, bins)}

        Batch-read multiple keys, and return a :class:`dict` of records. \
        For records that do not exist the value will be ``None``.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: an optional :class:`dict` with fields:

        .. hlist::
            :columns: 1

            * **timeout** read timeout in milliseconds

        :return: a :class:`dict` of :ref:`aerospike_record_tuple` keyed on the \
                 matching *primary key*.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                keys = [
                  ('test', 'demo', 1),
                  ('test', 'demo', 2),
                  ('test', 'demo', 3),
                  ('test', 'demo', 4)
                ]
                records = client.get_many(keys)
                print records
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)

        .. note::

            We expect to see something like:

            .. code-block:: python

                {
                  1: (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 1, 'ttl': 2592000}, {'age': 1, 'name': u'Name1'}), 
                  2: (('test', 'demo', 2, bytearray(b'\xaejQ_7\xdeJ\xda\xccD\x96\xe2\xda\x1f\xea\x84\x8c:\x92p')), {'gen': 1, 'ttl': 2592000}, {'age': 2, 'name': u'Name2'}), 
                  3: (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 1, 'ttl': 2592000}, {'age': 3, 'name': u'Name3'}), 
                  4: None
                }


    .. method:: exists_many(keys[, policy]) -> {primary_key: meta}

        Batch-read metadata for multiple keys, and return it as a :class:`dict`. \
        For records that do not exist the value will be ``None``.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: an optional :class:`dict` with fields:

        .. hlist::
            :columns: 1

            * **timeout** read timeout in milliseconds

        :return: a :class:`dict` of :ref:`aerospike_record_tuple` keyed on the \
                 matching *primary key*.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                keys = [
                  ('test', 'demo', 1),
                  ('test', 'demo', 2),
                  ('test', 'demo', 3),
                  ('test', 'demo', 4)
                ]
                records = client.exists_many(keys)
                print records
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)

        .. note::

            We expect to see something like:

            .. code-block:: python

                {
                  1: {'gen': 2, 'ttl': 2592000},
                  2: {'gen': 7, 'ttl': 1337},
                  3: {'gen': 9, 'ttl': 543},
                  4: None
                }


    .. method:: select_many(keys, bins[, policy]) -> {primary_key: (key. meta, bins)}

        Batch-read multiple keys, and return a :class:`dict` of records. \
        For records that do not exist the value will be ``None``. For records \
        which do exist, but for which the selected bins do not exist the *bins* \
        value will be ``{}``. Each of the *bins* will be filtered as specified.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param list bins: the bin names to select from the matching records.
        :param dict policy: an optional :class:`dict` with fields:

        .. hlist::
            :columns: 1

            * **timeout** read timeout in milliseconds

        :return: a :class:`dict` of :ref:`aerospike_record_tuple` keyed on the \
                 matching *primary key*.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                keys = [
                  ('test', 'demo', 1),
                  ('test', 'demo', 2),
                  ('test', 'demo', 3),
                  ('test', 'demo', 4)
                ]
                records = client.select_many(keys, [u'name'])
                print records
                client.close()
            except Exception as e:
                print("error: {0}".format(e), file=sys.stderr)
                sys.exit(1)

        .. note::

            We expect to see something like:

            .. code-block:: python

                {
                  1: (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 1, 'ttl': 2592000}, {'name': u'Name1'}),
                  2: (('test', 'demo', 2, bytearray(b'\xaejQ_7\xdeJ\xda\xccD\x96\xe2\xda\x1f\xea\x84\x8c:\x92p')), {'gen': 1, 'ttl': 2592000}, {'name': u'Name2'}),
                  3: (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 1, 'ttl': 2592000}, {'name': u'Name3'}),
                  4: None
                }


    .. rubric:: Scans

    .. method:: scan(namespace[, set]) -> Scan

        Return a :class:`aerospike.Scan` object to be used for executing scans \
        over a specified *set* (which can be ommitted or `None`) in a *namespace*.

        :param str namespace: a list of :ref:`aerospike_key_tuple`.
        :param str set: optional specified set name, otherwise the entire \
          *namespace* will be scanned.
        :return: an :py:class:`aerospike.Scan` class.


    .. rubric:: Queries

    .. method:: query(namespace[, set]) -> Query

        Return a :class:`aerospike.Query` object to be used for executing queries \
        over a specified *set* (which can be ommitted or `None`) in a *namespace*.

        :param str namespace: a list of :ref:`aerospike_key_tuple`.
        :param str set: optional specified set name, otherwise the records \
          which are not part of any *set* will be queried (**Note**: this is \
          different from not providing the *set* in :meth:`scan`).
        :return: an :py:class:`aerospike.Query` class.


    .. rubric:: UDFs

    .. method:: udf_put(filename[, udf_type=aerospike.UDF_TYPE_LUA[, policy]])

        Register a UDF module with the cluster.

        :param str filename: the UDF module to be registered with the cluster.
        :param int udf_type: one of ``aerospike.UDF_TYPE_\*``
        :param dict policy: currently **timeout** in milliseconds is the available policy.


    .. method:: udf_remove(module[, policy])

        Register a UDF module with the cluster.

        :param str module: the UDF module to be deregistered from the cluster.
        :param dict policy: currently **timeout** in milliseconds is the available policy.


    .. method:: udf_list([policy]) -> []

        Return the list of UDF modules registered with the cluster.

        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`list`

        .. code-block:: python

            import aerospike

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()
            udfs = client.udf_list()
            print(udfs)

        .. note::

            We expect to see something like:

            .. code-block:: python

                [{'content': bytearray(b''),
                  'hash': bytearray(b'195e39ceb51c110950bd'),
                  'name': 'my_udf1.lua',
                  'type': 0},
                 {'content': bytearray(b''),
                  'hash': bytearray(b'8a2528e8475271877b3b'),
                  'name': 'stream_udf.lua',
                  'type': 0},
                 {'content': bytearray(b''),
                  'hash': bytearray(b'362ea79c8b64857701c2'),
                  'name': 'aggregate_udf.lua',
                  'type': 0},
                 {'content': bytearray(b''),
                  'hash': bytearray(b'635f47081431379baa4b'),
                  'name': 'module.lua',
                  'type': 0}]


    .. method:: udf_get(module[, language=aerospike.UDF_TYPE_LUA[, policy]]) -> str

        Return the content of a UDF module which is registered with the cluster.

        :param str module: the UDF module to read from the cluster.
        :param int udf_type: one of ``aerospike.UDF_TYPE_\*``
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`str`


.. _aerospike_key_tuple:

Key Tuple
---------

.. object:: key

    The key tuple which is sent and returned by various operations contains

    ``(namespace, set, primary key[, the record's RIPEMD-160 digest])``


.. _aerospike_record_tuple:

Record Tuple
------------

.. object:: record

    The record tuple ``(key, meta, bins)`` which is returned by various read operations.

    .. hlist::
        :columns: 1

        * *key* the tuple ``(namespace, set, primary key, the record's RIPEMD-160 digest)``
        * *meta* a dict containing  ``{'gen' : genration value, 'ttl': ttl value}``
        * *bins* a dict containing bin-name/bin-value pairs


.. _aerospike_write_policies:

Write Policies
--------------

.. object:: policy

    A :class:`dict` of optional write policies which are applicable to :meth:`Client.put`. See :ref:`aerospike_policies`.

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

     A :class:`dict` of optional read policies which are applicable to :meth:`Client.get`. See :ref:`aerospike_policies`.

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

     A :class:`dict` of optional operate policies which are applicable to :meth:`Client.append`, :meth:`Client.prepend`, :meth:`Client.increment`, :meth:`Client.operate`. See :ref:`aerospike_policies`.

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

     A :class:`dict` of optional remove policies which are applicable to :meth:`Client.remove`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values


