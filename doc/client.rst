.. _client:

.. currentmodule:: aerospike

=================================
Client Class --- :class:`Client`
=================================

:class:`Client`
===============

The client connects through a seed node (the address of a single node) to an
Aerospike database cluster. From the seed node, the client learns of the other
nodes and establishes connections to them. It also gets the partition map of
the cluster, which is how it knows where every record actually lives.

The client handles the connections, including re-establishing them ahead of
executing an operation. It keeps track of changes to the cluster through
a cluster-tending thread.

.. seealso::
    `Client Architecture
    <https://www.aerospike.com/docs/architecture/clients.html>`_ and
    `Data Distribution <https://www.aerospike.com/docs/architecture/data-distribution.html>`_.

.. class:: Client

    Example::

        from __future__ import print_function
        # import the module
        import aerospike
        from aerospike.exception import *
        import sys

        # Configure the client
        config = {
            'hosts': [ ('127.0.0.1', 3000) ]
        }

        # Create a client and connect it to the cluster
        try:
            client = aerospike.client(config).connect()
        except ClientError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))
            sys.exit(1)

        # Records are addressable via a tuple of (namespace, set, primary key)
        key = ('test', 'demo', 'foo')

        try:
            # Write a record
            client.put(key, {
                'name': 'John Doe',
                'age': 32
            })
        except RecordError as e:
            print("Error: {0} [{1}]".format(e.msg, e.code))

        # Read a record
        (key, meta, record) = client.get(key)

        # Close the connection to the Aerospike cluster
        client.close()


    .. method:: connect([username, password])

        Connect to the cluster. The optional *username* and *password* only
        apply when connecting to the Enterprise Edition of Aerospike.

        :param str username: a defined user with roles in the cluster. See :meth:`admin_create_user`.
        :param str password: the password will be hashed by the client using bcrypt.
        :raises: :exc:`~aerospike.exception.ClientError`.

        .. seealso:: `Security features article <https://www.aerospike.com/docs/guide/security.html>`_.

    .. method:: is_connected()

        States whether the most recent connection attempt to the cluster succeeded.

        :rtype: :class:`bool`

    .. method:: close()

        Close all connections to the cluster. It is recommended to explicitly \
        call this method when the program is done communicating with the cluster.

    .. method:: get(key[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :py:func:`tuple` consisting of *key*, *meta* and *bins*.  If the record \
        does not exist the *meta* data will be ``None``.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta, bins) = client.get(key)
                if meta != None:
                    print(key)
                    print('--------------------------')
                    print(meta)
                    print('--------------------------')
                    print(bins)
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: select(key, bins[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :py:func:`tuple` consisting of *key*, *meta* and *bins*, with the \
        specified bins projected. If the record does not exist the *meta* \
        data will be ``None``. Prior to Aerospike server 3.6.0, if a selected \
        bin does not exist its value will be ``None``. Starting with 3.6.0, if
        a bin does not exist it will not be present in the returned \
        :ref:`aerospike_record_tuple`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list bins: a list of bin names to select from the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta, bins) = client.select(key, ['name'])

                if meta != None:
                    print("name: ", bins.get('name'))
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: exists(key[, policy]) -> (key, meta)

        Check if a record with a given *key* exists in the cluster and return \
        the record as a :py:func:`tuple` consisting of *key* and *meta*.  If \
        the record  does not exist the *meta* data will be ``None``.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :rtype: :py:func:`tuple` (key, meta)

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
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
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: put(key, bins[, meta[, policy[, serializer]]])

        Write a record with a given *key* to the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param dict bins: a :class:`dict` of bin-name / bin-value pairs.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional :ref:`aerospike_write_policies`.
        :param serializer: optionally override the serialization mode of the
            client with one of the :ref:`aerospike_serialization_constants`. To
            use a class-level user-defined serialization function registered with
            :func:`aerospike.set_serializer` use :const:`aerospike.SERIALIZER_USER`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError

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
                    'f': 3.14159265359,
                    's': '!@#@#$QSDAsd;as'
                }
                client.put(key, bins,
                         policy={'key': aerospike.POLICY_KEY_SEND},
                         meta={'ttl':180})
                # adding a bin
                client.put(key, {'smiley': u"\ud83d\ude04"})
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. note:: Using Generation Policy

            The generation policy allows a record to be written only when the \
            generation is a specific value. In the following example, we only \
            want to write the record if no change has occurred since \
            :meth:`exists` was called.

            .. code-block:: python

                from __future__ import print_function
                import aerospike
                from aerospike.exception import *
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
                except RecordGenerationError:
                    print("put() failed due to generation policy mismatch")
                except AerospikeError as e:
                    print("Error: {0} [{1}]".format(e.msg, e.code))
                client.close()

    .. method:: touch(key[, val=0[, meta[, policy]]])

        Touch the given record, resetting its \
        `time-to-live <http://www.aerospike.com/docs/client/c/usage/kvs/write.html#change-record-time-to-live-ttl>`_ \
        and incrementing its generation.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param int val: the optional ttl in seconds, with ``0`` resolving to the default value in the server config.
        :param dict meta: optional record metadata to be set.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

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
        :param dict policy: optional :ref:`aerospike_remove_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            import aerospike

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            key = ('test', 'demo', 1)
            client.remove(key, {'retry': aerospike.POLICY_RETRY_ONCE
            client.close()


    .. method:: get_key_digest(ns, set, key) -> bytearray

        Calculate the digest of a particular key. See: :ref:`aerospike_key_tuple`.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param key: the primary key identifier of the record within the set.
        :type key: :py:class:`str` or :py:class:`int`
        :return: a RIPEMD-160 digest of the input tuple.
        :rtype: :class:`bytearray`

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
        :param dict policy: optional :ref:`aerospike_write_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

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
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.append(key, 'name', ' jr.', policy={'timeout': 1200})
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: prepend(key, bin, val[, meta[, policy]])

        Prepend the string value in *bin* with the string *val*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to prepend to the value of *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.prepend(key, 'name', 'Dr. ', policy={'timeout': 1200})
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: increment(key, bin, offset[, meta[, policy]])

        Increment the integer value in *bin* by the integer *val*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int offset: the integer by which to increment the value in the *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
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
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: operate(key, list[, meta[, policy]]) -> (key, meta, bins)

        Perform multiple bin operations on a record with a given *key*, \
        with write operations happening before read ops. In Aerospike server \
        versions prior to 3.6.0, non-existent bins being read will have a \
        ``None`` value. Starting with 3.6.0 non-existent bins will not be \
        present in the returned :ref:`aerospike_record_tuple`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: a :class:`list` of one or more bin operations, each \
            structured as the :class:`dict` \
            ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
            See :ref:`aerospike_operators`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note::

            Currently each :meth:`operate` call can only have one
            write operation per-bin. For example a single bin cannot be both
            appended and prepended in a single call.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
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
                      "bin": "title",
                      "val": "Mr."
                    },
                    {
                      "op" : aerospike.OPERATOR_APPEND,
                      "bin": "name",
                      "val": " jr."
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
                (key, meta, bins) = client.operate(key, list, policy={'timeout':500})

                print(key)
                print('--------------------------')
                print(meta)
                print('--------------------------')
                print(bins)
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. rubric:: Batch Operations

    .. method:: get_many(keys[, policy]) -> [ (key, meta, bins)]

        Batch-read multiple records, and return them as a :class:`list`. Any \
        record that does not exist will have a ``None`` value for metadata \
        and bins in the record tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: optional :ref:`aerospike_batch_policies`.
        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. seealso:: More information about the \
            `Batch Index <https://www.aerospike.com/docs/guide/batch.html>`_ \
            interface new to Aerospike server >= 3.6.0.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assume the fourth key has no matching record
                keys = [
                  ('test', 'demo', '1'),
                  ('test', 'demo', '2'),
                  ('test', 'demo', '3'),
                  ('test', 'demo', '4')
                ]
                records = client.get_many(keys)
                print records
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

                [
                  (('test', 'demo', '1', bytearray(b'ev\xb4\x88\x8c\xcf\x92\x9c \x0bo\xbd\x90\xd0\x9d\xf3\xf6\xd1\x0c\xf3')), {'gen': 1, 'ttl': 2592000}, {'age': 1, 'name': u'Name1'}),
                  (('test', 'demo', '2', bytearray(b'n\xcd7p\x88\xdcF\xe1\xd6\x0e\x05\xfb\xcbs\xa68I\xf0T\xfd')), {'gen': 1, 'ttl': 2592000}, {'age': 2, 'name': u'Name2'}),
                  (('test', 'demo', '3', bytearray(b'\x9f\xf2\xe3\xf3\xc0\xc1\xc3q\xb5$n\xf8\xccV\xa9\xed\xd91a\x86')), {'gen': 1, 'ttl': 2592000}, {'age': 3, 'name': u'Name3'}),
                  (('test', 'demo', '4', bytearray(b'\x8eu\x19\xbe\xe0(\xda ^\xfa\x8ca\x93s\xe8\xb3%\xa8]\x8b')), None, None)
                ]

        .. warning::

            The return type changed to :class:`list` starting with version 1.0.50.

        .. versionchanged:: 1.0.50


    .. method:: exists_many(keys[, policy]) -> [ (key, meta)]

        Batch-read metadata for multiple keys, and return it as a :class:`list`. \
        Any record that does not exist will have a ``None`` value for metadata in \
        the result tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: optional :ref:`aerospike_batch_policies`.
        :return: a :class:`list` of (key, metadata) :py:func:`tuple`.

        .. seealso:: More information about the \
            `Batch Index <https://www.aerospike.com/docs/guide/batch.html>`_ \
            interface new to Aerospike server >= 3.6.0.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assume the fourth key has no matching record
                keys = [
                  ('test', 'demo', '1'),
                  ('test', 'demo', '2'),
                  ('test', 'demo', '3'),
                  ('test', 'demo', '4')
                ]
                records = client.exists_many(keys)
                print records
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

               [
                  (('test', 'demo', '1', bytearray(b'ev\xb4\x88\x8c\xcf\x92\x9c \x0bo\xbd\x90\xd0\x9d\xf3\xf6\xd1\x0c\xf3')), {'gen': 2, 'ttl': 2592000}),
                  (('test', 'demo', '2', bytearray(b'n\xcd7p\x88\xdcF\xe1\xd6\x0e\x05\xfb\xcbs\xa68I\xf0T\xfd')), {'gen': 7, 'ttl': 1337}),
                  (('test', 'demo', '3', bytearray(b'\x9f\xf2\xe3\xf3\xc0\xc1\xc3q\xb5$n\xf8\xccV\xa9\xed\xd91a\x86')), {'gen': 9, 'ttl': 543}),
                  (('test', 'demo', '4', bytearray(b'\x8eu\x19\xbe\xe0(\xda ^\xfa\x8ca\x93s\xe8\xb3%\xa8]\x8b')), None)
               ]

        .. warning::

            The return type changed to :class:`list` starting with version 1.0.50.

        .. versionchanged:: 1.0.50


    .. method:: select_many(keys, bins[, policy]) -> {primary_key: (key, meta, bins)}

        Batch-read multiple records, and return them as a :class:`list`. Any \
        record that does not exist will have a ``None`` value for metadata \
        and bins in the record tuple. The *bins* will be filtered as specified.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param list bins: the bin names to select from the matching records.
        :param dict policy: optional :ref:`aerospike_batch_policies`.
        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. seealso:: More information about the \
            `Batch Index <https://www.aerospike.com/docs/guide/batch.html>`_ \
            interface new to Aerospike server >= 3.6.0.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assume the fourth key has no matching record
                keys = [
                  ('test', 'demo', None, bytearray(b'ev\xb4\x88\x8c\xcf\x92\x9c \x0bo\xbd\x90\xd0\x9d\xf3\xf6\xd1\x0c\xf3'),
                  ('test', 'demo', None, bytearray(b'n\xcd7p\x88\xdcF\xe1\xd6\x0e\x05\xfb\xcbs\xa68I\xf0T\xfd'),
                  ('test', 'demo', None, bytearray(b'\x9f\xf2\xe3\xf3\xc0\xc1\xc3q\xb5$n\xf8\xccV\xa9\xed\xd91a\x86'),
                  ('test', 'demo', None, bytearray(b'\x8eu\x19\xbe\xe0(\xda ^\xfa\x8ca\x93s\xe8\xb3%\xa8]\x8b')
                ]
                records = client.select_many(keys, [u'name'])
                print records
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

                [
                  (('test', 'demo', None, bytearray(b'ev\xb4\x88\x8c\xcf\x92\x9c \x0bo\xbd\x90\xd0\x9d\xf3\xf6\xd1\x0c\xf3'), {'gen': 1, 'ttl': 2592000}, {'name': u'Name1'}),
                  (('test', 'demo', None, bytearray(b'n\xcd7p\x88\xdcF\xe1\xd6\x0e\x05\xfb\xcbs\xa68I\xf0T\xfd'), {'gen': 1, 'ttl': 2592000}, {'name': u'Name2'}),
                  (('test', 'demo', None, bytearray(b'\x9f\xf2\xe3\xf3\xc0\xc1\xc3q\xb5$n\xf8\xccV\xa9\xed\xd91a\x86'), {'gen': 1, 'ttl': 2592000}, {'name': u'Name3'}),
                  (('test', 'demo', None, bytearray(b'\x8eu\x19\xbe\xe0(\xda ^\xfa\x8ca\x93s\xe8\xb3%\xa8]\x8b'), None, None)
                ]

        .. warning::

            The return type changed to :class:`list` starting with version 1.0.50.

        .. versionchanged:: 1.0.50


    .. rubric:: Scans

    .. method:: scan(namespace[, set]) -> Scan

        Return a :class:`aerospike.Scan` object to be used for executing scans \
        over a specified *set* (which can be omitted or ``None``) in a \
        *namespace*. A scan with a ``None`` set returns all the records in the \
        namespace.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the entire \
          *namespace* will be scanned.
        :return: an :py:class:`aerospike.Scan` class.


    .. rubric:: Queries

    .. method:: query(namespace[, set]) -> Query

        Return a :class:`aerospike.Query` object to be used for executing queries \
        over a specified *set* (which can be omitted or ``None``) in a *namespace*. \
        A query with a ``None`` set returns records which are **not in any \
        named set**. This is different than the meaning of a ``None`` set in \
        a scan.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the records \
          which are not part of any *set* will be queried (**Note**: this is \
          different from not providing the *set* in :meth:`scan`).
        :return: an :py:class:`aerospike.Query` class.


    .. rubric:: UDFs

    .. method:: udf_put(filename[, udf_type=aerospike.UDF_TYPE_LUA[, policy]])

        Register a UDF module with the cluster.

        :param str filename: the path to the UDF module to be registered with the cluster.
        :param int udf_type: one of ``aerospike.UDF_TYPE_\*``
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note::
            Register the UDF module and copy it to the Lua 'user_path', \
            a directory that should contain a copy of the modules registered \
            with the cluster.

            .. code-block:: python
                :emphasize-lines: 3,5

                config = {
                    'hosts': [ ('127.0.0.1', 3000)],
                    'lua': { 'user_path': '/path/to/lua/user_path'}}
                client = aerospike.client(config).connect()
                client.udf_put('/path/to/my_module.lua')
                client.close()

        .. versionchanged:: 1.0.45


    .. method:: udf_remove(module[, policy])

        Register a UDF module with the cluster.

        :param str module: the UDF module to be deregistered from the cluster.
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            client.udf_remove('my_module.lua')

        .. versionchanged:: 1.0.39


    .. method:: udf_list([policy]) -> []

        Return the list of UDF modules registered with the cluster.

        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`list`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()
            print(client.udf_list())
            client.close()

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

        .. versionchanged:: 1.0.39


    .. method:: udf_get(module[, language=aerospike.UDF_TYPE_LUA[, policy]]) -> str

        Return the content of a UDF module which is registered with the cluster.

        :param str module: the UDF module to read from the cluster.
        :param int udf_type: one of ``aerospike.UDF_TYPE_\*``
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 1.0.39


    .. method:: apply(key, module, function, args[, policy])

        Apply a registered (see :meth:`udf_put`) record UDF to a particular record.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the record identified by *key*.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_apply_policies`.
        :return: the value optionally returned by the UDF, one of :class:`str`,\
                 :class:`int`, :class:`float`, :class:`bytearray`, :class:`list`, :class:`dict`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Record UDF <http://www.aerospike.com/docs/guide/record_udf.html>`_ \
          and `Developing Record UDFs <http://www.aerospike.com/docs/udf/developing_record_udfs.html>`_.


    .. method:: scan_apply(ns, set, module, function[, args[, policy[, options]]]) -> int

        Initiate a background scan and apply a record UDF to each record matched by the scan.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be ``None`` if the entire namespace is to be scanned.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the scan.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_scan_policies`.
        :param dict options: the :ref:`aerospike_scan_options` that will apply to the scan.
        :rtype: :class:`int`
        :return: a job ID that can be used with :meth:`job_info` to track the status of the ``aerospike.JOB_SCAN``, as it runs in the background.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Record UDF <http://www.aerospike.com/docs/guide/record_udf.html>`_ \
          and `Developing Record UDFs <http://www.aerospike.com/docs/udf/developing_record_udfs.html>`_.


    .. method:: query_apply(ns, set, predicate, module, function[, args[, policy]]) -> int

        Initiate a background query and apply a record UDF to each record matched by the query.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be ``None`` if you want to query records in the *ns* which are in no set.
        :param tuple predicate: the :py:func:`tuple` produced by one of the :mod:`aerospike.predicates` methods.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the query.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_query_policies`.
        :rtype: :class:`int`
        :return: a job ID that can be used with :meth:`job_info` to track the status of the ``aerospike.JOB_QUERY`` , as it runs in the background.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Record UDF <http://www.aerospike.com/docs/guide/record_udf.html>`_ \
          and `Developing Record UDFs <http://www.aerospike.com/docs/udf/developing_record_udfs.html>`_.

        .. warning::

            This functionality will become available with a future release of the Aerospike server.

        .. versionadded:: 1.0.50


    .. method:: job_info(job_id, module[, policy]) -> dict

        Return the status of a job running in the background.

        :param int job_id: the job ID returned by :meth:`scan_apply` and :meth:`query_apply`.
        :param module: one of ``aerospike.JOB_SCAN`` or ``aerospike.JOB_QUERY``.
        :returns: a :class:`dict` with keys *status*, *records_read*, and \
          *progress_pct*. The value of *status* is one of ``aerospike.JOB_STATUS_*``. See: :ref:`aerospike_job_constants`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError
            import time

            config = {'hosts': [ ('127.0.0.1', 3000)]}
            client = aerospike.client(config).connect()
            try:
                # run the UDF 'add_val' in Lua module 'simple' on the records of test.demo
                job_id = client.scan_apply('test', 'demo', 'simple', 'add_val', ['age', 1])
                while True:
                    time.sleep(0.25)
                    response = client.job_info(job_id, aerospike.JOB_SCAN)
                    if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                        break
                print("Job ", str(job_id), " completed")
                print("Progress percentage : ", response['progress_pct'])
                print("Number of scanned records : ", response['records_read'])
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
            client.close()

        .. versionadded:: 1.0.50


    .. method:: scan_info(scan_id) -> dict

        Return the status of a scan running in the background.

        :param int scan_id: the scan ID returned by :meth:`scan_apply`.
        :returns: a :class:`dict` with keys *status*, *records_scanned*, and \
          *progress_pct*. The value of *status* is one of ``aerospike.SCAN_STATUS_*``. See: :ref:`aerospike_scan_constants`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. deprecated:: 1.0.50
            Use :meth:`job_info` instead.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike.exception import AerospikeError

            config = {'hosts': [ ('127.0.0.1', 3000)]}
            client = aerospike.client(config).connect()
            try:
                scan_id = client.scan_apply('test', 'demo', 'simple', 'add_val', ['age', 1])
                while True:
                    response = client.scan_info(scan_id)
                    if response['status'] == aerospike.SCAN_STATUS_COMPLETED or \
                       response['status'] == aerospike.SCAN_STATUS_ABORTED:
                        break
                if response['status'] == aerospike.SCAN_STATUS_COMPLETED:
                    print("Background scan successful")
                    print("Progress percentage : ", response['progress_pct'])
                    print("Number of scanned records : ", response['records_scanned'])
                    print("Background scan status : ", "SCAN_STATUS_COMPLETED")
                else:
                    print("Scan_apply failed")
            except AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
            client.close()


    .. rubric:: Info

    .. method:: index_string_create(ns, set, bin, index_name[, policy])

        Create a string index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 1.0.39


    .. method:: index_integer_create(ns, set, bin, index_name[, policy])

        Create an integer index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 1.0.39

    .. method:: index_list_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named *index_name* for either numeric or string values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a list.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING`` and ``aerospike.INDEX_NUMERIC``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. warning::

            This functionality will become available with a future release of the Aerospike server.

        .. versionadded:: 1.0.42

    .. method:: index_map_keys_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named *index_name* for either numeric or string values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the keys of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING`` and ``aerospike.INDEX_NUMERIC``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. warning::

            This functionality will become available with a future release of the Aerospike Interfaces with the batch index interface on server.

        .. versionadded:: 1.0.42

    .. method:: index_map_values_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named *index_name* for either numeric or string values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the values of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING`` and ``aerospike.INDEX_NUMERIC``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. warning::

            This functionality will become available with a future release of the Aerospike server.

        .. code-block:: python

            import aerospike
            from aerospike import predicates as p

            client = aerospike.client({ 'hosts': [ ('127.0.0.1', 3000)]}).connect()

            # assume the bin fav_movies in the set test.demo bin should contain
            # a dict { (str) _title_ : (int) _times_viewed_ }
            # create a secondary index for string values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_keys_create('test', 'demo', 'fav_movies', aerospike.INDEX_STRING, 'demo_fav_movies_titles_idx')
            # create a secondary index for integer values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_values_create('test', 'demo', 'fav_movies', aerospike.INDEX_NUMERIC, 'demo_fav_movies_views_idx')
            client.close()

        .. versionadded:: 1.0.42

    .. method:: index_2dsphere_create(ns, set, bin, index_name[, policy])

        Create a geospatial 2dsphere index with *index_name* on the *bin* in \
        the specified *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: The :class:`aerospike.Geo` class, and queries using \
            :meth:`aerospike.predicates.within`.

        .. versionadded:: 1.0.53


    .. method:: index_remove(ns, index_name[, policy])

        Remove the index with *index_name* from the namespace.

        :param str ns: the namespace in the aerospike cluster.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 1.0.39


    .. method:: get_nodes()  ->  []

        Return the list of hosts present in a connected cluster.

        :rtype: :class:`list`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            import aerospike

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            nodes = client.get_nodes()
            print(nodes)
            client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

                [ ( '127.0.0.1', 3000), ('127.0.0.1', 3010) ]

        .. versionchanged:: 1.0.41



     .. method:: info(command[, hosts[, policy]]) -> {}

        Send an info *command* to multiple nodes specified in a *hosts* list.

        :param str command: the info command.
        :param list hosts: a :class:`list` containing an *address*, *port* :py:func:`tuple`. Example: ``[('127.0.0.1', 3000)]``
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`dict`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.

        .. code-block:: python

            import aerospike

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            response = client.info(command)
            client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

                {'BB9581F41290C00': (None, '127.0.0.1:3000\n'), 'BC3581F41290C00': (None, '127.0.0.1:3010\n')}

        .. versionchanged:: 1.0.41


     .. method:: info_node(command, host[, policy]) -> str

        Send an info *command* to a single node specified by *host*.

        :param str command: the info command.
        :param tuple host: a :py:func:`tuple` containing an *address*, *port* pair. Example: ``('127.0.0.1', 3000)``
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.

        .. versionchanged:: 1.0.41


    .. rubric:: LList

    .. method:: llist(key, bin[, module]) -> LList

        Return a :class:`aerospike.LList` object on a specified *key* and *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param str bin: the name of the bin containing the :class:`~aerospike.LList`.
        :param str module: an optional UDF module that contains filtering \
                           functions to be used in conjunction with LList methods.
        :return: an :py:class:`aerospike.LList` class.
        :raises: a subclass of :exc:`~aerospike.exception.LDTError`.


    .. rubric:: Admin

    .. note::

        The admin methods implement the security features of the Enterprise \
        Edition of Aerospike. These methods will raise a \
        :exc:`~aerospike.exception.SecurityNotSupported` when the client is \
        connected to a Community Edition cluster (see
        :mod:`aerospike.exception`). \

        A user is validated by the client against the server whenever a \
        connection is established through the use of a username and password \
        (passwords hashed using bcrypt). \
        When security is enabled, each operation is validated against the \
        user\'s roles. Users are assigned roles, which are collections of \
        :ref:`aerospike_privilege_dict`.

        .. code-block:: python

            import aerospike
            from aerospike.exception import *
            import time

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect('ipji', 'life is good')

            try:
                dev_privileges = [{'code': aerospike.PRIV_READ}, {'code': aerospike.PRIV_READ_WRITE}]
                client.admin_create_role('dev_role', dev_privileges)
                client.admin_grant_privileges('dev_role', [{'code': aerospike.PRIV_READ_WRITE_UDF}])
                client.admin_create_user('dev', 'you young whatchacallit... idiot', ['dev_role'])
                time.sleep(1)
                print(client.admin_query_user('dev'))
                print(admin_query_users())
            except AdminError as e:
                print("Error [{0}]: {1}".format(e.code, e.msg))
            client.close()

        .. seealso:: `Security features article <https://www.aerospike.com/docs/guide/security.html>`_.


    .. method:: admin_create_role(role, privileges[, policy])

        Create a custom, named *role* containing a :class:`list` of
        *privileges*.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_drop_role(role[, policy])

        Drop a custom *role*.

        :param str role: the name of the role.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_grant_privileges(role, privileges[, policy])

        Add *privileges* to a *role*.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_revoke_privileges(role, privileges[, policy])

        Remove *privileges* from a *role*.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_query_role(role[, policy]) -> []

        Get the :class:`list` of privileges associated with a *role*.

        :param str role: the name of the role.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`list` of :ref:`aerospike_privilege_dict`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_query_roles([policy]) -> {}

        Get all named roles and their privileges.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of :ref:`aerospike_privilege_dict` keyed by role name.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_create_user(username, password, roles[, policy])

        Create a user with a specified *username* and grant it *roles*.

        :param str username: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param list roles: the list of role names assigned to the user.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_drop_user(username[, policy])

        Drop the user with a specified *username* from the cluster.

        :param str username: the username to be dropped from the aerospike cluster.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_change_password(username, password[, policy])

        Change the *password* of the user *username*. This operation can only \
        be performed by that same user.

        :param str username: the username.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_set_password(username, password[, policy])

        Set the *password* of the user *username* by a user administrator.

        :param str username: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_grant_roles(username, roles[, policy])

        Add *roles* to the user *username*.

        :param str username: the username to be granted the roles.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_revoke_roles(username, roles[, policy])

        Remove *roles* from the user *username*.

        :param str username: the username to have the roles revoked.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_query_user (username[, policy]) -> []

        Return the list of roles granted to the specified user *username*.

        :param str username: the username to query for.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`list` of role names.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


    .. method:: admin_query_users ([policy]) -> {}

        Return the :class:`dict` of users, with their roles keyed by username.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of roles keyed by username.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. versionchanged:: 1.0.44


.. _aerospike_key_tuple:

Key Tuple
---------

.. object:: key

    The key tuple which is sent and returned by various operations contains

    ``(namespace, set, primary key[, the record's RIPEMD-160 digest])``

    .. hlist::
        :columns: 1

        * *namespace* the :class:`str` name of the namespace, which must be \
          preconfigured on the cluster.
        * *set* the :class:`str` name of the set. Will be created automatically \
          if it does not exist.
        * *primary key* the value by which the client-side application \
          identifies the record, which can be of type :class:`str`, :class:`int` \
          or :class:`bytearray`.
        * *digest* the first three parts of the tuple get hashed through \
          RIPEMD-160, and the digest used by the clients and cluster nodes \
          to locate the record. A key tuple is also valid if it has the \
          digest part filled and the primary key part set to ``None``.

    .. code-block:: python

        >>> client = aerospike.client(config).connect()
        >>> client.put(('test','demo','oof'), {'id':0, 'a':1})
        >>> (key, meta, bins) = client.get(('test','demo','oof'))
        >>> key
        ('test', 'demo', None, bytearray(b'\ti\xcb\xb9\xb6V#V\xecI#\xealu\x05\x00H\x98\xe4='))
        >>> (key2, meta2, bins2) = client.get(key)
        >>> bins2
        {'a': 1, 'id': 0}
        >>> client.close()

    .. seealso:: `Data Model: Key / Digest <https://www.aerospike.com/docs/architecture/data-model.html#key-digest>`_.

    .. versionchanged:: 1.0.47


.. _aerospike_record_tuple:

Record Tuple
------------

.. object:: record

    The record tuple ``(key, meta, bins)`` which is returned by various read operations.

    .. hlist::
        :columns: 1

        * *key* the :ref:`aerospike_key_tuple`.
        * *meta* a :class:`dict` containing  ``{'gen' : genration value, 'ttl': ttl value}``.
        * *bins* a :class:`dict` containing bin-name/bin-value pairs.

    .. seealso:: `Data Model: Record <https://www.aerospike.com/docs/architecture/data-model.html#records>`_.


.. _unicode_handling:

Unicode Handling
----------------

Both :class:`str` and :func:`unicode` values are converted by the
client into UTF-8 encoded strings for storage on the aerospike server.
Read methods such as :meth:`~aerospike.Client.get`,
:meth:`~aerospike.Client.query`, :meth:`~aerospike.Client.scan` and
:meth:`~aerospike.Client.operate` will return that data as UTF-8 encoded
:class:`str` values. To get a :func:`unicode` you will need to manually decode.

.. warning::

    Prior to release 1.0.43 read operations always returned strings as :func:`unicode`.

.. code-block:: python

    >>> client.put(key, { 'name': 'Dr. Zeta Alphabeta', 'age': 47})
    >>> (key, meta, record) = client.get(key)
    >>> type(record['name'])
    <type 'str'>
    >>> record['name']
    'Dr. Zeta Alphabeta'
    >>> client.put(key, { 'name': unichr(0x2603), 'age': 21})
    >>> (key, meta, record) = client.get(key)
    >>> type(record['name'])
    <type 'str'>
    >>> record['name']
    '\xe2\x98\x83'
    >>> print(record['name'])
    ☃
    >>> name = record['name'].decode('utf-8')
    >>> type(name)
    <type 'unicode'>
    >>> name
    u'\u2603'
    >>> print(name)
    ☃


.. versionchanged:: 1.0.43

.. _aerospike_write_policies:

Write Policies
--------------

.. object:: policy

    A :class:`dict` of optional write policies which are applicable to :meth:`~Client.put`. See :ref:`aerospike_policies`.

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

     A :class:`dict` of optional read policies which are applicable to :meth:`~Client.get`. See :ref:`aerospike_policies`.

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

     A :class:`dict` of optional operate policies which are applicable to :meth:`~Client.append`, :meth:`~Client.prepend`, :meth:`~Client.increment`, :meth:`~Client.operate`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values
        * **replica** one of the `aerospike_POLICY_REPLICA_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gabce1fb468ee9cbfe54b7ab834cec79ab>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values
        * **consistency_level** one of the `aerospike.POLICY_CONSISTENCY_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga34dbe8d01c941be845145af643f9b5ab>`_ values

.. _aerospike_apply_policies:

Apply Policies
--------------

.. object:: policy

    A :class:`dict` of optional apply policies which are applicable to :meth:`~Client.apply`, and :class:`~aerospike.LList` methods. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values


.. _aerospike_remove_policies:

Remove Policies
---------------

.. object:: policy

     A :class:`dict` of optional remove policies which are applicable to :meth:`~Client.remove`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** write timeout in milliseconds
        * **commit_level** one of the `aerospike.POLICY_COMMIT_LEVEL_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga17faf52aeb845998e14ba0f3745e8f23>`_ values
        * **key** one of the `aerospike.POLICY_KEY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9c8a79b2ab9d3812876c3ec5d1d50ec>`_ values
        * **retry** one of the `aerospike.POLICY_RETRY_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#gaa9730980a8b0eda8ab936a48009a6718>`_ values
        * **gen** one of the `aerospike.POLICY_GEN_* <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html#ga38c1a40903e463e5d0af0141e8c64061>`_ values


.. _aerospike_batch_policies:

Batch Policies
--------------

.. object:: policy

     A :class:`dict` of optional batch policies which are applicable to \
     :meth:`~aerospike.Client.get_many`, :meth:`~aerospike.Client.exists_many` \
     and :meth:`~aerospike.Client.select_many`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** read timeout in milliseconds


.. _aerospike_info_policies:

Info Policies
-------------

.. object:: policy

     A :class:`dict` of optional info policies which are applicable to \
     :meth:`~aerospike.Client.info`, :meth:`~aerospike.Client.info_node` \
     and index operations. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** read timeout in milliseconds


.. _aerospike_admin_policies:

Admin Policies
--------------

.. object:: policy

     A :class:`dict` of optional admin policies which are applicable to admin (security) operations. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** read timeout in milliseconds


.. _aerospike_privilege_dict:

Privilege Objects
-----------------

.. object:: privilege

    A :class:`dict` describing a privilege associated with a specific role.

    .. hlist::
        :columns: 1

        * **code** one of the `aerospike.PRIV_* <http://www.aerospike.com/apidocs/c/dd/d3f/as__admin_8h.html#a3abfbabd6287af263860154d044b44b3>`_ values
        * **ns** optional namespace to which the privilege applies, otherwise the privilege applies globally.
        * **set** optional set within the *ns* to which the privilege applies, otherwise to the entire namespace.

    Example:

    ``{'code': aerospike.PRIV_READ, 'ns': 'test', 'set': 'demo'}``

