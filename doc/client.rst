.. _client:

.. currentmodule:: aerospike

================================
Client Class --- :class:`Client`
================================

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
        from aerospike import exception as ex
        import sys

        # Configure the client
        config = {
            'hosts': [ ('127.0.0.1', 3000) ]
        }

        # Optionally set policies for various method types
        write_policies = {'total_timeout': 2000, 'max_retries': 0}
        read_policies = {'total_timeout': 1500, 'max_retries': 1}
        policies = {'write': write_policies, 'read': read_policies}
        config['policies'] = policies

        # Create a client and connect it to the cluster
        try:
            client = aerospike.client(config).connect()
        except ex.ClientError as e:
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
        except ex.RecordError as e:
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
        :raises: :exc:`~aerospike.exception.ClientError`, for example when a connection cannot be \
                 established to a seed node (any single node in the cluster from which the client \
                 learns of the other nodes).

        .. seealso:: `Security features article <https://www.aerospike.com/docs/guide/security/index.html>`_.

    .. method:: is_connected()

        Tests the connections between the client and the nodes of the cluster.
        If the result is ``False``, the client will require another call to
        :meth:`~aerospike.Client.connect`.

        :rtype: :class:`bool`

        .. versionchanged:: 2.0.0

    .. method:: close()

        Close all connections to the cluster. It is recommended to explicitly \
        call this method when the program is done communicating with the cluster.

    .. method:: get(key[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :py:func:`tuple` consisting of *key*, *meta* and *bins*.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: :exc:`~aerospike.exception.RecordNotFound`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = {'hosts': [('127.0.0.1', 3000)]}
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
            except ex.RecordNotFound:
                print("Record not found:", key)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. warning::

            The client has been changed to raise a :py:exc:`~aerospike.exception.RecordNotFound` \
            exception when :meth:`~aerospike.Client.get` does not find the \
            record. Code that used to check for ``meta != None`` should be \
            modified.

        .. versionchanged:: 2.0.0


    .. method:: select(key, bins[, policy]) -> (key, meta, bins)

        Read a record with a given *key*, and return the record as a \
        :py:func:`tuple` consisting of *key*, *meta* and *bins*, with the \
        specified bins projected. Prior to Aerospike server 3.6.0, if a selected \
        bin does not exist its value will be :py:obj:`None`. Starting with 3.6.0, if
        a bin does not exist it will not be present in the returned \
        :ref:`aerospike_record_tuple`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list bins: a list of bin names to select from the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: :exc:`~aerospike.exception.RecordNotFound`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                # assuming a record with such a key exists in the cluster
                key = ('test', 'demo', 1)
                (key, meta, bins) = client.select(key, ['name'])
                print("name: ", bins.get('name'))
            except ex.RecordNotFound:
                print("Record not found:", key)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. warning::

            The client has been changed to raise a :py:exc:`~aerospike.exception.RecordNotFound` \
            exception when :meth:`~aerospike.Client.select` does not find the \
            record. Code that used to check for ``meta != None`` should be \
            modified.

        .. versionchanged:: 2.0.0


    .. method:: exists(key[, policy]) -> (key, meta)

        Check if a record with a given *key* exists in the cluster and return \
        the record as a :py:func:`tuple` consisting of *key* and *meta*.  If \
        the record  does not exist the *meta* data will be :py:obj:`None`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.
        :rtype: :py:func:`tuple` (key, meta)
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
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
            except ex.RecordNotFound:
                print("Record not found:", key)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. versionchanged:: 2.0.3


    .. method:: put(key, bins[, meta[, policy[, serializer]]])

        Write a record with a given *key* to the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param dict bins: a :class:`dict` of bin-name / bin-value pairs.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_write_policies`.
        :param serializer: optionally override the serialization mode of the
            client with one of the :ref:`aerospike_serialization_constants`. To
            use a class-level user-defined serialization function registered with
            :func:`aerospike.set_serializer` use :const:`aerospike.SERIALIZER_USER`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex

            config = {
                'hosts': [ ('127.0.0.1', 3000) ],
                'total_timeout': 1500
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
                # removing a bin
                client.put(key, {'i': aerospike.null()})
            except ex.AerospikeError as e:
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
                from aerospike import exception as ex
                import sys

                config = { 'hosts': [ ('127.0.0.1',3000)]}
                client = aerospike.client(config).connect()

                try:
                    (key, meta) = client.exists(('test','test','key1'))
                    print(meta)
                    print('============')
                    client.put(('test','test','key1'), {'id':1,'a':2},
                        meta={'gen': 33},
                        policy={'gen':aerospike.POLICY_GEN_EQ})
                    print('Record written.')
                except ex.RecordGenerationError:
                    print("put() failed due to generation policy mismatch")
                except ex.AerospikeError as e:
                    print("Error: {0} [{1}]".format(e.msg, e.code))
                client.close()

    .. method:: touch(key[, val=0[, meta[, policy]]])

        Touch the given record, setting its \
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
            client.touch(key, 120, policy={'total_timeout': 100})
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

        .. deprecated:: 2.0.1
            use the function :func:`aerospike.calc_digest` instead.

    .. index::
        single: Bin Operations

    .. _aerospike_bin_operations:

    .. rubric:: Bin Operations


    .. method:: remove_bin(key, list[, meta[, policy]])

        Remove a list of bins from a record with a given *key*. Equivalent to \
        setting those bins to :meth:`aerospike.null` with a :meth:`~aerospike.Client.put`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: the bins names to be removed from the record.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
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
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.append(key, 'name', ' jr.', policy={'total_timeout': 1200})
            except ex.AerospikeError as e:
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
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.prepend(key, 'name', 'Dr. ', policy={'total_timeout': 1200})
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()


    .. method:: increment(key, bin, offset[, meta[, policy]])

        Increment the integer value in *bin* by the integer *val*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int offset: the value by which to increment the value in *bin*.
        :type offset: :py:class:`int` or :py:class:`float`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
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
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

    .. method:: list_append(key, bin, val[, meta[, policy]])

        Append a single element to a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param val: :py:class:`int`, :py:class:`str`, \
                   :py:class:`float`, :py:class:`bytearray`, :py:class:`list`, \
                   :py:class:`dict`. An unsupported type will be serialized.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_extend(key, bin, items[, meta[, policy]])

        Extend the list value in *bin* with the given *items*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param list items: the items to append the list in *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_insert(key, bin, index, val[, meta[, policy]])

        Insert an element at the specified *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index where the value should be inserted.
        :param val: :py:class:`int`, :py:class:`str`, \
                   :py:class:`float`, :py:class:`bytearray`, :py:class:`list`, \
                   :py:class:`dict`. An unsupported type will be serialized.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_insert_items(key, bin, index, items[, meta[, policy]])

        Insert the *items* at the specified *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index where the items should be inserted.
        :param list items: the items to insert into the list in *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_pop(key, bin, index[, meta[, policy]]) -> val

        Remove and get back a list element at a given *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the index position in the list element which should be removed and returned.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a single list element.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_pop_range(key, bin, index, count[, meta[, policy]]) -> val

        Remove and get back list elements at a given *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the index of first element in a range which should be removed and returned.
        :param int count: the number of elements in the range.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :class:`list` of elements.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_remove(key, bin, index[, meta[, policy]])

        Remove a list element at a given *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the index position in the list element which should be removed.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_remove_range(key, bin, index, count[, meta[, policy]])

        Remove list elements at a given *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the index of first element in a range which should be removed.
        :param int count: the number of elements in the range.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_clear(key, bin[, meta[, policy]])

        Remove all the elements from a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_set(key, bin, index, val[, meta[, policy]])

        Set list element *val* at the specified *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index where the value should be set.
        :param val: :py:class:`int`, :py:class:`str`, \
                   :py:class:`float`, :py:class:`bytearray`, :py:class:`list`, \
                   :py:class:`dict`. An unsupported type will be serialized.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_get(key, bin, index[, meta[, policy]]) -> val

        Get the list element at the specified *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index where the value should be set.
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: the list elements at the given index.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_get_range(key, bin, index, count[, meta[, policy]]) -> val

        Get the list of *count* elements starting at a specified *index* of a list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index where the value should be set.
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: a :class:`list` of elements.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_trim(key, bin, index, count[, meta[, policy]]) -> val

        Remove elements from the list which are not within the range starting at the given *index* plus *count*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int index: the position in the index marking the start of the range.
        :param int index: the index position of the first element in a range which should not be removed.
        :param int count: the number of elements in the range.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: a :class:`list` of elements.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: list_size(key, bin[, meta[, policy]]) -> count

        Count the number of elements in the list value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: a :class:`int`.

        .. note:: Requires server version >= 3.7.0

        .. versionadded:: 1.0.59

    .. method:: map_set_policy(key, bin, map_policy)

        Set the map policy for the given *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict map_policy: :ref:`aerospike_map_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_put(key, bin, map_key, val[, map_policy, [, meta[, policy]]])

        Add the given *map_key*/*value* pair to the map record specified by *key* and *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param val: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`, :py:class:`list`, \
           :py:class:`dict`. An unsupported type will be serialized.
        :param dict map_policy: optional :ref:`aerospike_map_policies`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_put_items(key, bin, items[, map_policy, [, meta[, policy]]])

        Add the given *items* dict of key/value pairs to the map record specified by *key* and *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict items: key/value pairs.
        :param dict map_policy: optional :ref:`aerospike_map_policies`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_increment(key, bin, map_key, incr[, map_policy, [, meta[, policy]]])
 
        Increment the value of the map entry by given *incr*. Map entry is specified by *key*, *bin* and *map_key*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param incr: :py:class:`int` or :py:class:`float`
        :param dict map_policy: optional :ref:`aerospike_map_policies`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_decrement(key, bin, map_key, decr[, map_policy, [, meta[, policy]]])

        Decrement the value of the map entry by given *decr*. Map entry is specified by *key*, *bin* and *map_key*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param decr: :py:class:`int` or :py:class:`float`
        :param dict map_policy: optional :ref:`aerospike_map_policies`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_size(key, bin[, meta[, policy]]) -> count

        Return the size of the map specified by *key* and *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: a :class:`int`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_clear(key, bin[, meta[, policy]])

        Remove all entries from the map specified by *key* and *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_key(key, bin, map_key, return_type[, meta[, policy]])

        Remove and optionally return first map entry from the map specified by *key* and *bin* which matches given *map_key*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_key_list(key, bin, list, return_type[, meta[, policy]][, meta[, policy]])

        Remove and optionally return map entries from the map specified by *key* and *bin* which have keys that match the given *list* of keys.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param list: :py:class:`list` the list of keys to match
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_key_range(key, bin, map_key, range, return_type[, meta[, policy]])

        Remove and optionally return map entries from the map specified by *key* and *bin* identified by the key range (*map_key* inclusive, *range* exclusive).

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param range: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_value(key, bin, val, return_type[, meta[, policy]])

        Remove and optionally return map entries from the map specified by *key* and *bin* which have a value matching *val* parameter.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param val: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_value_list(key, bin, list, return_type[, meta[, policy]])

        Remove and optionally return map entries from the map specified by *key* and *bin* which have a value matching the *list* of values.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param list: :py:class:`list` the list of values to match
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_value_range(key, bin, val, range, return_type[, meta[, policy]])

        Remove and optionally return map entries from the map specified by *key* and *bin* identified by the value range (*val* inclusive, *range* exclusive).

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param val: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param range: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_index(key, bin, index, return_type[, meta[, policy]])

        Remove and optionally return the map entry from the map specified by *key* and *bin* at the given *index* location.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param index: :py:class:`int` the index location of the map entry
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_index_range(key, bin, index, range, return_type[, meta[, policy]])

        Remove and optionally return the map entries from the map specified by *key* and *bin* starting at the given *index* location and removing *range* number of items.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param index: :py:class:`int` the index location of the first map entry to remove
        :param range: :py:class:`int` the number of items to remove from the map 
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_rank(key, bin, rank, return_type[, meta[, policy]])

        Remove and optionally return the map entry from the map specified by *key* and *bin* with a value that has the given *rank*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param rank: :py:class:`int` the rank of the value of the entry in the map
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_remove_by_rank_range(key, bin, rank, range, return_type[, meta[, policy]])

        Remove and optionally return the map entries from the map specified by *key* and *bin* which have a value rank starting at *rank* and removing *range* number of items.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param rank: :py:class:`int` the rank of the value of the first map entry to remove
        :param range: :py:class:`int` the number of items to remove from the map 
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_key(key, bin, map_key, return_type[, meta[, policy]])
       
        Return map entry from the map specified by *key* and *bin* which has a key that matches the given *map_key*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key:  :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_key_range(key, bin, map_key, range, return_type[, meta[, policy]])

        Return map entries from the map specified by *key* and *bin* identified by the key range (*map_key* inclusive, *range* exclusive).

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param map_key: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param range: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_value(key, bin, val, return_type[, meta[, policy]])

        Return map entries from the map specified by *key* and *bin* which have a value matching *val* parameter.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param val: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_value_range(key, bin, val, range, return_type[, meta[, policy]])

        Return map entries from the map specified by *key* and *bin* identified by the value range (*val* inclusive, *range* exclusive).

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param val: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param range: :py:class:`int`, :py:class:`str`, \
           :py:class:`float`, :py:class:`bytearray`. An unsupported type will be serialized.
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_index(key, bin, index, return_type[, meta[, policy]])

        Return the map entry from the map specified by *key* and *bin* at the given *index* location.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param index: :py:class:`int` the index location of the map entry
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

 
        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_index_range(key, bin, index, range, return_type[, meta[, policy]])

        Return the map entries from the map specified by *key* and *bin* starting at the given *index* location and removing *range* number of items.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param index: :py:class:`int` the index location of the first map entry to remove
        :param range: :py:class:`int` the number of items to remove from the map 
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_rank(key, bin, rank, return_type[, meta[, policy]])

        Return the map entry from the map specified by *key* and *bin* with a value that has the given *rank*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param rank: :py:class:`int` the rank of the value of the entry in the map
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4

    .. method:: map_get_by_rank_range(key, bin, rank, range, return_type[, meta[, policy]])
   
        Return the map entries from the map specified by *key* and *bin* which have a value rank starting at *rank* and removing *range* number of items.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param rank: :py:class:`int` the rank of the value of the first map entry to remove
        :param range: :py:class:`int` the number of items to remove from the map 
        :param return_type: :py:class:`int` :ref:`map_return_types`
        :param dict meta: unused for this operation
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.
        :return: depends on return_type parameter

        .. note:: Requires server version >= 3.8.4

        .. versionadded:: 2.0.4


    .. method:: operate(key, list[, meta[, policy]]) -> (key, meta, bins)

        Perform multiple bin operations on a record with a given *key*, \
        In Aerospike server \
        versions prior to 3.6.0, non-existent bins being read will have a \
        :py:obj:`None` value. Starting with 3.6.0 non-existent bins will not be \
        present in the returned :ref:`aerospike_record_tuple`. \
        The returned record tuple will only contain one entry per bin, even if multiple operations were performed on the bin.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: a :class:`list` of one or more bin operations, each \
            structured as the :class:`dict` \
            ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
            See :ref:`aerospike_operators`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of 
            :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
            :const:`aerospike.TTL_DONT_UPDATE`
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note::
            In version `2.1.3` the return format of certain bin entries for this method, **only in cases when a map operation specifying a `return_type` is used**, has changed. Bin entries for map operations using "return_type" of aerospike.MAP_RETURN_KEY_VALUE will now return \
            a bin value of a list of keys and corresponding values, rather than a list of key/value tuples. See the following code block for details.

        .. code-block:: python

            # pre 2.1.3 formatting of key/value bin value
            [('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')]

            # >= 2.1.3 formatting
            ['key1', 'val1', 'key2', 'val2', 'key3', 'val3']

        .. note::

            :meth:`operate` can now have multiple write operations on a single
            bin.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                client.put(key, {'age': 25, 'career': 'delivery boy'})
                ops = [
                    {
                      "op" : aerospike.OPERATOR_INCR,
                      "bin": "age",
                      "val": 1000
                    },
                    {
                      "op" : aerospike.OPERATOR_WRITE,
                      "bin": "name",
                      "val": "J."
                    },
                    {
                      "op" : aerospike.OPERATOR_PREPEND,
                      "bin": "name",
                      "val": "Phillip "
                    },
                    {
                      "op" : aerospike.OPERATOR_APPEND,
                      "bin": "name",
                      "val": " Fry"
                    },
                    {
                      "op" : aerospike.OPERATOR_READ,
                      "bin": "name"
                    },
                    {
                      "op" : aerospike.OPERATOR_READ,
                      "bin": "career"
                    }
                ]
                (key, meta, bins) = client.operate(key, ops, {'ttl':360}, {'total_timeout':500})

                print(key)
                print('--------------------------')
                print(meta)
                print('--------------------------')
                print(bins) # will display all bins selected by OPERATOR_READ operations
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. note::

            :const:`~aerospike.OPERATOR_TOUCH` should only ever combine with
            :const:`~aerospike.OPERATOR_READ`, for example to implement LRU
            expiry on the records of a set.

        .. warning::

            Having *val* associated with :const:`~aerospike.OPERATOR_TOUCH` is deprecated.
            Use the meta *ttl* field instead.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
            import sys

            config = { 'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            try:
                key = ('test', 'demo', 1)
                ops = [
                    {
                      "op" : aerospike.OPERATOR_TOUCH,
                    },
                    {
                      "op" : aerospike.OPERATOR_READ,
                      "bin": "name"
                    }
                ]
                (key, meta, bins) = client.operate(key, ops, {'ttl':1800})
                print("Touched the record for {0}, extending its ttl by 30m".format(bins))
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. versionchanged:: 2.1.3

    .. method:: operate_ordered(key, list[, meta[, policy]]) -> (key, meta, bins)

            Perform multiple bin operations on a record with the results being \
            returned as a list of (bin-name, result) tuples. The order of the \
            elements in the list will correspond to the order of the operations \
            from the input parameters.

            :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
            :param list list: a :class:`list` of one or more bin operations, each \
                structured as the :class:`dict` \
                ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
                See :ref:`aerospike_operators`.
            :param dict meta: optional record metadata to be set, with field
                ``'ttl'`` set to :class:`int` number of seconds or one of 
                :const:`aerospike.TTL_NAMESPACE_DEFAULT`, :const:`aerospike.TTL_NEVER_EXPIRE`, 
                :const:`aerospike.TTL_DONT_UPDATE`
            :param dict policy: optional :ref:`aerospike_operate_policies`.
            :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
            :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

            .. note::
                In version `2.1.3` the return format of bin entries for this method, **only in cases when a map operation specifying a `return_type` is used**, has changed. Map operations using "return_type" of aerospike.MAP_RETURN_KEY_VALUE will now return \
                a bin value of a list of keys and corresponding values, rather than a list of key/value tuples. See the following code block for details. In addition, some operations which did not return a value in versions <= 2.1.2 will now return a response.

            .. code-block:: python

                # pre 2.1.3 formatting of key/value bin value
                [('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')]

                # >= 2.1.3 formatting
                ['key1', 'val1', 'key2', 'val2', 'key3', 'val3']

            .. code-block:: python

                from __future__ import print_function
                import aerospike
                from aerospike import exception as ex
                import sys

                config = { 'hosts': [('127.0.0.1', 3000)] }
                client = aerospike.client(config).connect()
                
                try:
                    key = ('test', 'demo', 1)
                    policy = {
                        'total_timeout': 1000,
                        'key': aerospike.POLICY_KEY_SEND,
                        'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
                    }

                    llist = [{"op": aerospike.OPERATOR_APPEND,
                              "bin": "name",
                              "val": "aa"},
                             {"op": aerospike.OPERATOR_READ,
                              "bin": "name"},
                             {"op": aerospike.OPERATOR_INCR,
                              "bin": "age",
                              "val": 3}]

                    client.operate_ordered(key, llist, {}, policy)
                except ex.AerospikeError as e:
                    print("Error: {0} [{1}]".format(e.msg, e.code))
                    sys.exit(1)
                finally:
                    client.close()
                    
        .. versionadded:: 2.0.2
        .. versionchanged:: 2.1.3

    .. index::
        single: Batch Operations

    .. _aerospike_batch_operations:

    .. rubric:: Batch Operations

    .. method:: get_many(keys[, policy]) -> [ (key, meta, bins)]

        Batch-read multiple records, and return them as a :class:`list`. Any \
        record that does not exist will have a :py:obj:`None` value for metadata \
        and bins in the record tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: optional :ref:`aerospike_batch_policies`.
        :return: a :class:`list` of :ref:`aerospike_record_tuple`.
        :raises: a :exc:`~aerospike.exception.ClientError` if the batch is too big.

        .. seealso:: More information about the \
            `Batch Index <https://www.aerospike.com/docs/guide/batch.html>`_ \
            interface new to Aerospike server >= 3.6.0.

        .. code-block:: python

            from __future__ import print_function
            import aerospike
            from aerospike import exception as ex
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
            except ex.AerospikeError as e:
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
        Any record that does not exist will have a :py:obj:`None` value for metadata in \
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
            from aerospike import exception as ex
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
            except ex.AerospikeError as e:
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
        record that does not exist will have a :py:obj:`None` value for metadata \
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
            from aerospike import exception as ex
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
            except ex.AerospikeError as e:
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
        over a specified *set* (which can be omitted or :py:obj:`None`) in a \
        *namespace*. A scan with a :py:obj:`None` set returns all the records in the \
        namespace.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the entire \
          *namespace* will be scanned.
        :return: an :py:class:`aerospike.Scan` class.


    .. rubric:: Queries

    .. method:: query(namespace[, set]) -> Query

        Return a :class:`aerospike.Query` object to be used for executing queries \
        over a specified *set* (which can be omitted or :py:obj:`None`) in a *namespace*. \
        A query with a :py:obj:`None` set returns records which are **not in any \
        named set**. This is different than the meaning of a :py:obj:`None` set in \
        a scan.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the records \
          which are not part of any *set* will be queried (**Note**: this is \
          different from not providing the *set* in :meth:`scan`).
        :return: an :py:class:`aerospike.Query` class.

    .. index::
        single: UDF Operations

    .. _aerospike_udf_operations:

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
        :param str set: the set name. Should be :py:obj:`None` if the entire namespace is to be scanned.
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
        :param str set: the set name. Should be :py:obj:`None` if you want to query records in the *ns* which are in no set.
        :param tuple predicate: the :py:func:`tuple` produced by one of the :mod:`aerospike.predicates` methods.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the query.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_write_policies`.
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
            from aerospike import exception as ex
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
            except ex.AerospikeError as e:
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
            from aerospike import exception as ex

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
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
            client.close()


    .. index::
        single: Info Operations

    .. _aerospike_info_operations:

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

        Create an index named *index_name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a list.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

        .. versionadded:: 1.0.42

    .. method:: index_map_keys_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named *index_name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the keys of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

        .. versionadded:: 1.0.42

    .. method:: index_map_values_create(ns, set, bin, index_datatype, index_name[, policy])

        Create an index named *index_name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the values of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

        .. code-block:: python

            import aerospike

            client = aerospike.client({ 'hosts': [ ('127.0.0.1', 3000)]}).connect()

            # assume the bin fav_movies in the set test.demo bin should contain
            # a dict { (str) _title_ : (int) _times_viewed_ }
            # create a secondary index for string values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_keys_create('test', 'demo', 'fav_movies', aerospike.INDEX_STRING, 'demo_fav_movies_titles_idx')
            # create a secondary index for integer values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_values_create('test', 'demo', 'fav_movies', aerospike.INDEX_NUMERIC, 'demo_fav_movies_views_idx')
            client.close()

        .. versionadded:: 1.0.42

    .. method:: index_geo2dsphere_create(ns, set, bin, index_name[, policy])

        Create a geospatial 2D spherical index with *index_name* on the *bin* \
        in the specified *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: :class:`aerospike.GeoJSON`, :mod:`aerospike.predicates`

        .. note:: Requires server version >= 3.7.0

        .. code-block:: python

            import aerospike

            client = aerospike.client({ 'hosts': [ ('127.0.0.1', 3000)]}).connect()
            client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')
            client.close()

        .. versionadded:: 1.0.53


    .. method:: index_remove(ns, index_name[, policy])

        Remove the index with *index_name* from the namespace.

        :param str ns: the namespace in the aerospike cluster.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 1.0.39


    .. method:: get_nodes() -> []

        Return the list of hosts present in a connected cluster.

        :return: a :class:`list` of node address tuples.
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

                [('127.0.0.1', 3000), ('127.0.0.1', 3010)]

        .. versionchanged:: 1.0.41

        .. warning:: ``get_nodes`` will not work when using TLS

     .. method:: info(command[, hosts[, policy]]) -> {}

        .. deprecated:: 3.0.0
            Use :meth:`info_node` to send a request to a single node, or :meth:`info_all` to send a request to the entire cluster. Sending requests to specific nodes can be better handled with a simple Python function such as:

            .. code-block:: python

                def info_to_host_list(client, request, hosts, policy=None):
                    output = {}
                    for host in hosts:
                        try:
                            response = client.info_node(request, host, policy)
                            output[host] = response
                        except Exception as e:
                            #  Handle the error gracefully here
                            output[host] = e
                    return output

        Send an info *command* to all nodes in the cluster and filter responses to only include nodes specified in a *hosts* list.

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

        .. versionchanged:: 3.0.0

     .. method:: info_all(command[, policy]]) -> {}

        Send an info *command* to all nodes in the cluster to which the client is connected. If any of the individual requests fail, this will raise an exception.

        :param str command: the info command.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`dict`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.

        .. code-block:: python

            import aerospike

            config = {'hosts': [('127.0.0.1', 3000)] }
            client = aerospike.client(config).connect()

            response = client.info_all(command)
            client.close()

        .. note::

            We expect to see something like:

            .. code-block:: python

                {'BB9581F41290C00': (None, '127.0.0.1:3000\n'), 'BC3581F41290C00': (None, '127.0.0.1:3010\n')}

        .. versionadded:: 3.0.0

     .. method:: info_node(command, host[, policy]) -> str

        Send an info *command* to a single node specified by *host*.

        :param str command: the info command.
        :param tuple host: a :py:func:`tuple` containing an *address*, *port* , optional *tls-name* . Example: ``('127.0.0.1', 3000)`` or when using TLS ``('127.0.0.1', 4333, 'server-tls-name')``. In order to send an info request when TLS is enabled, the *tls-name* must be present.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.

        .. versionchanged:: 3.0.0

        .. warning:: for client versions < 3.0.0 ``info_node`` will not work when using TLS

    .. method:: has_geo()  ->  bool

        Check whether the connected cluster supports geospatial data and indexes.

        :rtype: :class:`bool`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionadded:: 1.0.53

    .. method:: shm_key()  ->  int

        Expose the value of the shm_key for this client if shared-memory cluster tending is enabled, 

        :rtype: :class:`int` or :py:obj:`None`

        .. versionadded:: 1.0.56

    .. method:: truncate(namespace, set, nanos[, policy])

        Remove records in specified namespace/set efficiently.  This method is many orders of magnitude
        faster than deleting records one at a time.  Works with Aerospike Server versions >= 3.12.
 
        This asynchronous server call may return before the truncation is complete.  The user can still
        write new records after the server returns because new records will have last update times
        greater than the truncate cutoff (set at the time of truncate call)
        
        :param str namespace: The namespace on which the truncation operation should be performed.
        :param str set: The set to truncate. Pass in ``None`` to indicate that all records in the namespace should be truncated.
        :param long nanos:  A cutoff threshold indicating that records last updated before the threshold will be removed.Units are in nanoseconds since unix epoch (1970-01-01). A value of ``0`` indicates that all records in the set should be truncated regardless of update time. The value must not be in the future.
        :param dict policy: Optional :ref:`aerospike_info_policies`
        :rtype: Status indicating the success of the operation.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionadded:: 2.0.11
        .. code-block:: python

            import aerospike
            import time

            client = aerospike.client({'hosts': [('localhost', 3000)]}).connect()

            # Store 10 items in the database
            for i in range(10):
                key = ('test', 'truncate', i)
                record = {'item': i}
                client.put(key, record)

            time.sleep(2)
            current_time = time.time()
            # Convert the current time to nanoseconds since epoch
            threshold_ns = int(current_time * 10 ** 9)

            time.sleep(2)  # Make sure some time passes before next round of additions

            # Store another 10 items into the database
            for i in range(10, 20):
                key = ('test', 'truncate', i)
                record = {'item': i}
                client.put(key, record)

            # Store a record in the 'test' namespace without a set
            key = ('test', None, 'no set')
            record = ({'item': 'no set'})
            client.put(key, record)

            # Remove all items created before the threshold time
            # The first 10 records we added will be removed by this call.
            # The second 10 will remain.
            client.truncate('test', 'truncate', threshold_ns)


            # Remove all records from test/truncate.
            # After this the record with key ('test', None, 'no set') still exists
            client.truncate('test', 'truncate', 0)

            # Remove all records from the test namespace
            client.truncate('test', None, 0)

            client.close()

    .. index::
        single: Admin Operations

    .. _aerospike_admin_operations:

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
            from aerospike import exception as ex
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
            except ex.AdminError as e:
                print("Error [{0}]: {1}".format(e.code, e.msg))
            client.close()

        .. seealso:: `Security features article <https://www.aerospike.com/docs/guide/security/index.html>`_.


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
          digest part filled and the primary key part set to :py:obj:`None`.

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

    .. seealso:: `Data Model: Keys and Digests <https://www.aerospike.com/docs/architecture/data-model.html#records>`_.

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

    A :class:`dict` of optional write policies which are applicable to :meth:`~Client.put`, :meth:`~Client.query_apply`. :meth:`~Client.remove_bin`.

    .. hlist::
        :columns: 1

        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``0``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep. Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **key** one of the ``aerospike.POLICY_KEY_*`` values such as :data:`aerospike.POLICY_KEY_DIGEST`
        * **exists** one of the ``aerospike.POLICY_EXISTS_*`` values such as :data:`aerospike.POLICY_EXISTS_CREATE`
        * **gen** one of the ``aerospike.POLICY_GEN_*`` values such as :data:`aerospike.POLICY_GEN_IGNORE`
        * **commit_level** one of the ``aerospike.POLICY_COMMIT_LEVEL_*`` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **durable_delete** boolean value: True to perform durable delete (requires Enterprise server version >= 3.10)

.. _aerospike_read_policies:

Read Policies
-------------

.. object:: policy

    A :class:`dict` of optional read policies which are applicable to :meth:`~Client.get`, :meth:`~Client.exists`, :meth:`~Client.select`.

    .. hlist::
        :columns: 1


        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``2``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep. Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **deserialize**
            | :class:`bool` Should raw bytes representing a list or map be deserialized to a list or dictionary.
            | Set to `False` for backup programs that just need access to raw bytes.
            | Default: ``True``
        * **linearize_read**
            | :class:`bool`
            | Force reads to be linearized for server namespaces that support CP mode. Setting this policy to ``True`` requires an Enterprise server with version 4.0.0 or greater.
            | Default: ``False``
        * **key** one of the ``aerospike.POLICY_KEY_*`` values such as :data:`aerospike.POLICY_KEY_DIGEST`
        * **consistency_level** one of the ``aerospike.POLICY_CONSISTENCY_*`` values such as :data:`aerospike.POLICY_CONSISTENCY_ONE`
        * **replica** one of the ``aerospike.POLICY_REPLICA_*`` values such as :data:`aerospike.POLICY_REPLICA_MASTER`

.. _aerospike_operate_policies:

Operate Policies
----------------

.. object:: policy

    A :class:`dict` of optional operate policies which are applicable to :meth:`~Client.append`, :meth:`~Client.prepend`, :meth:`~Client.increment`, :meth:`~Client.operate`, and atomic list and map operations.

    .. hlist::
        :columns: 1

        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``0``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **linearize_read**
            | :class:`bool`
            | Force reads to be linearized for server namespaces that support CP mode. Setting this policy to ``True`` requires an Enterprise server with version 4.0.0 or greater.
            | Default: ``False``
        * **key** one of the ``aerospike.POLICY_KEY_*`` values such as :data:`aerospike.POLICY_KEY_DIGEST`
        * **gen** one of the ``aerospike.POLICY_GEN_*`` values such as :data:`aerospike.POLICY_GEN_IGNORE`
        * **replica** one of the ``aerospike.POLICY_REPLICA_*`` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
        * **commit_level** one of the ``aerospike.POLICY_COMMIT_LEVEL_*`` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **consistency_level** one of the ``aerospike.POLICY_CONSISTENCY_*`` values such as :data:`aerospike.POLICY_CONSISTENCY_ONE`
        * **durable_delete** boolean value: True to perform durable delete (requires Enterprise server version >= 3.10)

.. _aerospike_apply_policies:

Apply Policies
--------------

.. object:: policy

    A :class:`dict` of optional apply policies which are applicable to :meth:`~Client.apply`.

    .. hlist::
        :columns: 1

        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``0``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep. Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **linearize_read**
            | :class:`bool`
            | Force reads to be linearized for server namespaces that support CP mode. Setting this policy to ``True`` requires an Enterprise server with version 4.0.0 or greater.
            | Default: ``False``
        * **key** one of the ``aerospike.POLICY_KEY_*`` values such as :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** one of the ``aerospike.POLICY_COMMIT_LEVEL_*`` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **durable_delete** boolean value: True to perform durable delete (requires Enterprise server version >= 3.10)

.. _aerospike_remove_policies:

Remove Policies
---------------

.. object:: policy

    A :class:`dict` of optional remove policies which are applicable to :meth:`~Client.remove`.

    .. hlist::
        :columns: 1

        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``0``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep. Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **key** one of the ``aerospike.POLICY_KEY_*`` values such as :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** one of the ``aerospike.POLICY_COMMIT_LEVEL_*`` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **gen** one of the ``aerospike.POLICY_GEN_*`` values such as :data:`aerospike.POLICY_GEN_IGNORE`
        * **max_retries** integer, number of times to retry the operation if it fails due to netowrk error. Default `2`
        * **durable_delete** boolean value: True to perform durable delete (requires Enterprise server version >= 3.10)

.. _aerospike_batch_policies:

Batch Policies
--------------

.. object:: policy

    A :class:`dict` of optional batch policies which are applicable to \
     :meth:`~aerospike.Client.get_many`, :meth:`~aerospike.Client.exists_many` \
     and :meth:`~aerospike.Client.select_many`.

    .. hlist::
        :columns: 1

        * **max_retries**
            | An :class:`int`. Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | **WARNING**: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times
            | if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes which sets max_retries = `0`;
            |
            | Default: ``2``
        * **sleep_between_retries**
            | An :class:`int`. Milliseconds to sleep between retries. Enter zero to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout**
            | An :class:`int`. Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not zero and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is zero, there will be no socket idle limit.
            |
            | Default: ``0``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``1000``
        * **linearize_read**
            | :class:`bool`
            | Force reads to be linearized for server namespaces that support CP mode. Setting this policy to ``True`` requires an Enterprise server with version 4.0.0 or greater.
            | Default: ``False``
        * **consistency_level** one of the ``aerospike.POLICY_CONSISTENCY_*`` values such as :data:`aerospike.POLICY_CONSISTENCY_ONE`
        * **concurrent** :class:`bool` Determine if batch commands to each server are run in parallel threads. Default `False`
        * **allow_inline** :class:`bool` . Allow batch to be processed immediately in the server's receiving thread when the server deems it to be appropriate.  If `False`, the batch will always be processed in separate transaction threads.  This field is only relevant for the new batch index protocol. Default `True`.
        * **send_set_name** :class:`bool` Send set name field to server for every key in the batch for batch index protocol. This is only necessary when authentication is enabled and security roles are defined on a per set basis. Default: `False`
        * **deserialize** :class:`bool` Should raw bytes be deserialized to as_list or as_map. Set to `False` for backup programs that just need access to raw bytes. Default: `True`

.. _aerospike_info_policies:

Info Policies
-------------

.. object:: policy

    A :class:`dict` of optional info policies which are applicable to \
     :meth:`~aerospike.Client.info`, :meth:`~aerospike.Client.info_node` \
     and index operations.

    .. hlist::
        :columns: 1

        * **timeout** read timeout in milliseconds


.. _aerospike_admin_policies:

Admin Policies
--------------

.. object:: policy

    A :class:`dict` of optional admin policies which are applicable to admin (security) operations.

    .. hlist::
        :columns: 1

        * **timeout** admin operation timeout in milliseconds


.. _aerospike_map_policies:

Map Policies
------------

.. object:: policy

    A :class:`dict` of optional map policies which are applicable to map operations.

    .. hlist::
        :columns: 1

        * **map_write_mode** write mode for the map. Valid values: aerospike.MAP_UPDATE, aerospike.MAP_UPDATE_ONLY, aerospike.MAP_CREATE_ONLY
        * **map_order** ordering to maintain for the map entries. Valid values: aerospike.MAP_UNORDERED, aerospike.MAP_KEY_ORDERED, aerospike.MAP_KEY_VALUE_ORDERED


.. _aerospike_privilege_dict:

Privilege Objects
-----------------

.. object:: privilege

    A :class:`dict` describing a privilege associated with a specific role.

    .. hlist::
        :columns: 1

        * **code** one of the ``aerospike.PRIV_*`` values such as :data:`aerospike.PRIV_READ`
        * **ns** optional namespace to which the privilege applies, otherwise the privilege applies globally.
        * **set** optional set within the *ns* to which the privilege applies, otherwise to the entire namespace.

    Example:

    ``{'code': aerospike.PRIV_READ, 'ns': 'test', 'set': 'demo'}``

