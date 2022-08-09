.. _client:

.. currentmodule:: aerospike

==========================================
:class:`aerospike.Client` --- Client Class
==========================================

Overview
========

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

Boilerplate Code For Examples
-----------------------------

Assume every in-line example runs this code beforehand:

.. include:: examples/boilerplate.py
    :code: python

Basic example:

::

    # Write a record
    client.put(keyTuple, {'name': 'John Doe', 'age': 32})

    # Read a record
    (key, meta, record) = client.get(keyTuple)

Methods
=======

.. _aerospike_connection_operations:

Connection
----------

.. class:: Client

    .. method:: connect([username, password])

        Connect to the cluster. The optional *username* and *password* only
        apply when connecting to the Enterprise Edition of Aerospike.

        :param str username: a defined user with roles in the cluster. See :meth:`admin_create_user`.
        :param str password: the password will be hashed by the client using bcrypt.
        :raises: :exc:`~aerospike.exception.ClientError`, for example when a connection cannot be \
                 established to a seed node (any single node in the cluster from which the client \
                 learns of the other nodes).
        
        .. note::
            Python client 5.0.0 and up will fail to connect to Aerospike server 4.8.x or older.
            If you see the error "-10, ‘Failed to connect’", please make sure you are using server 4.9 or later.

        .. seealso:: `Security features article <https://www.aerospike.com/docs/guide/security/index.html>`_.

    .. method:: is_connected()

        Tests the connections between the client and the nodes of the cluster.
        If the result is ``False``, the client will require another call to
        :meth:`~aerospike.connect`.

        :rtype: :class:`bool`

        .. versionchanged:: 2.0.0

    .. method:: close()

        Close all connections to the cluster. It is recommended to explicitly \
        call this method when the program is done communicating with the cluster.

Record Operations
-----------------

.. class:: Client
    :noindex:

    .. method:: put(key, bins: dict[, meta: dict[, policy: dict[, serializer=aerospike.SERIALIZER_PYTHON]]])

        Create a new record, or remove / add bins to a record.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict bins: contains bin name-value pairs of the record.
        :param dict meta: see :ref:`metadata_dict`.
        :param dict policy: see :ref:`aerospike_write_policies`.

        :param serializer: override the serialization mode of the client \
            with one of the :ref:`aerospike_serialization_constants`.
            To use a class-level, user-defined serialization function registered with :func:`aerospike.set_serializer`, \
            use :const:`aerospike.SERIALIZER_USER`.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        Example:

        .. include:: examples/put.py
            :code: python

        .. note::
            Version >= 5.0.0 supports Aerospike expressions for record operations. See :ref:`aerospike_operation_helpers.expressions`.
            Requires server version >= 5.2.0. 

    .. method:: exists(key[, policy: dict]) -> (key, meta)

        Check if a record with a given key exists in the cluster.
        
        Returns the record's key and metadata in a tuple.
        
        If the record does not exist, the tuple's metadata will be :py:obj:`None`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: see :ref:`aerospike_read_policies`.

        :rtype: `tuple` (key, meta)

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/exists.py
            :code: python

        .. versionchanged:: 2.0.3

    .. method:: get(key[, policy: dict]) -> (key, meta, bins)

        Returns a record with a given key.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict policy: see :ref:`aerospike_read_policies`.

        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        
        :raises: :exc:`~aerospike.exception.RecordNotFound`.

        .. include:: examples/get.py
            :code: python

        .. versionchanged:: 2.0.0

    .. method:: select(key, bins: list[, policy: dict]) -> (key, meta, bins)

        Returns specific bins of a record.
        
        If a bin does not exist, it will not show up in the returned :ref:`aerospike_record_tuple`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list bins: a list of bin names to select from the record.
        :param dict policy: optional :ref:`aerospike_read_policies`.

        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        
        :raises: :exc:`~aerospike.exception.RecordNotFound`.

        .. include:: examples/select.py
            :code: python
            
        .. versionchanged:: 2.0.0

    .. method:: touch(key[, val=0[, meta: dict[, policy: dict]]])

        Touch the given record, setting its time-to-live and incrementing its generation.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param int val: ttl in seconds, with ``0`` resolving to the default value in the server config.
        :param dict meta: see :ref:`metadata_dict`
        :param dict policy: see :ref:`aerospike_operate_policies`.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/touch.py
            :code: python

    .. method:: remove(key[meta: dict[, policy: dict]])

        Remove a record matching the *key* from the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict meta: a dictonary with the expected generation of the record.
        :param dict policy: see :ref:`aerospike_remove_policies`. May be passed as a keyword argument.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/remove.py
            :code: python

    .. method:: get_key_digest(ns, set, key) -> bytearray

        Calculate the digest of a particular key. See :ref:`aerospike_key_tuple`.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param key: the primary key identifier of the record within the set.
        :type key: :py:class:`str` or :py:class:`int`

        :return: a RIPEMD-160 digest of the input tuple.
        
        :rtype: :class:`bytearray`

        .. include:: examples/get_key_digest.py
            :code: python

        .. deprecated:: 2.0.1
            use the function :func:`aerospike.calc_digest` instead.

    .. method:: remove_bin(key, list[, meta: dict[, policy: dict]])

        Remove a list of bins from a record with a given *key*. Equivalent to \
        setting those bins to :meth:`aerospike.null` with a :meth:`~aerospike.put`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: the bins names to be removed from the record.
        :param dict meta: see :ref:`metadata_dict`.
        :param dict policy: optional :ref:`aerospike_write_policies`.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/remove_bin.py
            :code: python

    .. index::
        single: Batch Operations

Batch Operations
----------------

.. class:: Client
    :noindex:

    .. method:: get_many(keys[, policy: dict]) -> [(key, meta, bins)]

        Batch-read multiple records, and return them as a :class:`list`.
        
        Any record that does not exist will have a :py:obj:`None` value for metadata \
        and bins in the record tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of :ref:`aerospike_record_tuple`.
        
        :raises: a :exc:`~aerospike.exception.ClientError` if the batch is too big.

        .. include:: examples/get_many.py
            :code: python

        .. note::
            Client version >= 5.0.0 supports Aerospike expressions for batch operations.
            
            See :ref:`aerospike_operation_helpers.expressions`.
            
            Requires server version >= 5.2.0.

    .. method:: exists_many(keys[, policy: dict]) -> [ (key, meta)]

        Batch-read metadata for multiple keys.

        Any record that does not exist will have a :py:obj:`None` value for metadata in \
        their tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of (key, metadata) :class:`tuple` for each record.

        .. include:: examples/exists_many.py
            :code: python

    .. method:: select_many(keys, bins: list[, policy: dict]) -> [(key, meta, bins), ...]}

        Batch-read specific bins from multiple records.
        
        Any record that does not exist will have a :py:obj:`None` value for metadata and bins in its tuple.
        
        :param list keys: a list of :ref:`aerospike_key_tuple` to read from.
        :param list bins: a list of bin names to read from the records.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. include:: examples/select_many.py
            :code: python

    .. method:: batch_get_ops(keys, ops, meta, policy: dict) -> [ (key, meta, bins)]

        Batch-read multiple records, and return them as a :class:`list`.
        
        Any record that does not exist will have a exception type value as metadata \
        and :py:obj:`None` value as bins in the record tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param list ops: a list of operations to apply.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of :ref:`aerospike_record_tuple`.
        
        :raises: a :exc:`~aerospike.exception.ClientError` if the batch is too big.

        .. include:: examples/batch_get_ops.py
            :code: python

        .. note::
            Version >= 5.0.0 Supports aerrospike expressions for batch operations see :ref:`aerospike_operation_helpers.expressions`.
            Requires server version >= 5.2.0.


    .. method:: batch_write(batch_records: BatchRecords, [policy: dict]) -> BatchRecords

        .. note:: Requires server version >= 6.0.0.

        Write/read multiple records for specified batch keys in one batch call.
        This method allows different sub-commands for each key in the batch.
        The resulting records and status are set in ``batch_records`` record and result fields.

        .. note:: This function modifies the ``batch_records`` parameter.

        :param BatchRecords batch_records: A :class:`BatchRecords` object used to specify the operations to carry out.
        :param dict policy: Optional aerospike batch policy :ref:`aerospike_batch_policies`.
        :return: A reference to the batch_records argument of type :class:`BatchRecords <aerospike_helpers.batch.records>`.
        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: More information about the \
            batch helpers :ref:`aerospike_operation_helpers.batch`

        .. include:: examples/batch_write.py
            :code: python

    .. method:: batch_operate(keys: list, ops: list, [policy_batch: dict], [policy_batch_write: dict]) -> BatchRecords

        .. note:: Requires server version >= 6.0.0.

        Perform the same read/write operations on multiple keys.

        :param list keys: The keys to operate on.
        :param list ops: List of operations to apply.
        :param dict policy_batch: Optional aerospike batch policy :ref:`aerospike_batch_policies`.
        :param dict policy_batch_write: Optional aerospike batch write policy :ref:`aerospike_batch_write_policies`.
        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.
        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_operate.py
            :code: python

    .. method:: batch_apply(keys: list, module: str, function: str, args: list, [policy_batch: dict], [policy_batch_apply: dict]) -> BatchRecords

        .. note:: Requires server version >= 6.0.0.

        Apply UDF (user defined function) on multiple keys.

        :param list keys: The keys to operate on.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the record identified by *key*.
        :param list args: the arguments to the UDF.
        :param dict policy_batch: Optional aerospike batch policy :ref:`aerospike_batch_policies`.
        :param dict policy_batch_apply: Optional aerospike batch apply policy :ref:`aerospike_batch_apply_policies`.
        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.
        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_apply.py
            :code: python


    .. method:: batch_remove(keys: list, [policy_batch: dict], [policy_batch_remove: dict]) -> BatchRecords

        .. note:: Requires server version >= 6.0.0.

        Remove multiple records by key.

        :param list keys: The keys to remove.
        :param dict policy_batch: Optional aerospike batch policy :ref:`aerospike_batch_policies`.
        :param dict policy_batch_remove: Optional aerospike batch remove policy :ref:`aerospike_batch_remove_policies`.
        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.
        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_remove.py
            :code: python

    .. index::
        single: String Operations

String Operations
-----------------

.. class:: Client
    :noindex:

    .. note:: Please see :mod:`aerospike_helpers.operations.operations` for the new way to use string operations.

    .. method:: append(key, bin, val[, meta: dict[, policy: dict]])

        Append the string *val* to the string value in *bin*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to append to the value of *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of the :ref:`TTL_CONSTANTS`, \
            and ``'gen'`` set to :class:`int` generation number to compare.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            client.put(keyTuple, {'bin1': 'Martin Luther King'})
            client.append(keyTuple, 'bin1', ' jr.')
            (_, _, bins) = client.get(keyTuple)
            print(bins) # Martin Luther King jr.

    .. method:: prepend(key, bin, val[, meta: dict[, policy: dict]])

        Prepend the string value in *bin* with the string *val*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to prepend to the value of *bin*.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of the :ref:`TTL_CONSTANTS`, \
            and ``'gen'`` set to :class:`int` generation number to compare.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            client.put(keyTuple, {'bin1': 'Freeman'})
            client.prepend(keyTuple, 'bin1', ' Gordon ')
            (_, _, bins) = client.get(keyTuple)
            print(bins) # Gordon Freeman

    .. index::
        single: Numeric Operations

Numeric Operations
------------------
.. class:: Client
    :noindex:
    
    .. note:: Please see :mod:`aerospike_helpers.operations.operations` for the new way to use numeric operations using the operate command.

    .. method:: increment(key, bin, offset[, meta: dict[, policy: dict]])

        Increment the integer value in *bin* by the integer *val*.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param int offset: the value by which to increment the value in *bin*.
        :type offset: :py:class:`int` or :py:class:`float`
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of the :ref:`TTL_CONSTANTS`, \
            and ``'gen'`` set to :class:`int` generation number to compare.
        :param dict policy: optional :ref:`aerospike_operate_policies`. Note: the ``exists`` policy option may not be: ``aerospike.POLICY_EXISTS_CREATE_OR_REPLACE`` nor ``aerospike.POLICY_EXISTS_REPLACE``
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            # Start with 100 lives
            client.put(keyTuple, {'lives': 100})

            # Gain health
            client.increment(keyTuple, 'lives', 10)
            (key, meta, bins) = client.get(keyTuple)
            print(bins) # 110

            # Take damage
            client.increment(keyTuple, 'lives', -90)
            (key, meta, bins) = client.get(keyTuple)
            print(bins) # 20

    .. index::
        single: List Operations

List Operations
---------------

    .. note:: Please see :mod:`aerospike_helpers.operations.list_operations` for the new way to use list operations.
                Old style list operations are deprecated. The docs for old style list operations were removed in client 6.0.0.
                The code supporting these methods will be removed in a coming release.

    .. index::
        single: Map Operations

Map Operations
--------------

    .. note:: Please see :mod:`aerospike_helpers.operations.map_operations` for the new way to use map operations.
                Old style map operations are deprecated. The docs for old style map operations were removed in client 6.0.0.
                The code supporting these methods will be removed in a coming release.

    .. index::
        single: Multi-Ops

Single-Record Transactions
--------------------------

.. class:: Client
    :noindex:

    .. method:: operate(key, operations: list[, meta: dict[, policy: dict]]) -> (key, meta, bins)

        Performs an atomic transaction, with multiple bin operations, against a single record with a given *key*.

        Starting with Aerospike server version 3.6.0, non-existent bins are not present in the returned :ref:`aerospike_record_tuple`. \
        The returned record tuple will only contain one element per bin, even if multiple operations were performed on the bin. \
        (In Aerospike server versions prior to 3.6.0, non-existent bins being read will have a \
        :py:obj:`None` value. )

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list operations: a :class:`list` of one or more bin operations, each \
            structured as the :class:`dict` \
            ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
            See :ref:`aerospike_operation_helpers.operations`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of the :ref:`TTL_CONSTANTS`, \
            and ``'gen'`` set to :class:`int` generation number to compare.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            from aerospike_helpers.operations import operations

            # Add name, update age, and return attributes
            client.put(keyTuple, {'age': 25, 'career': 'delivery boy'})
            ops = [
                operations.increment("age", 1000),
                operations.write("name", "J."),
                operations.prepend("name", "Phillip "),
                operations.append("name", " Fry"),
                operations.read("name"),
                operations.read("career"),
                operations.read("age")
            ]
            (key, meta, bins) = client.operate(key, ops)

            print(key) # ('test', 'demo', None, bytearray(b'...'))
            # The generation should only increment once
            # A transaction is *atomic*
            print(meta) # {'ttl': 2592000, 'gen': 2}
            print(bins) # Will display all bins selected by read operations
            # {'name': 'Phillip J. Fry', 'career': 'delivery boy', 'age': 1025}

        .. note::
            Version >= 5.0.0 Supports aerrospike expressions for transactions see :ref:`aerospike_operation_helpers.expressions`.
            Requires server version >= 5.2.0.

            .. code-block:: python

                import aerospike
                from aerospike_helpers import expressions as exp
                from aerospike_helpers.operations import list_operations, operations
                from aerospike import exception as ex
                import sys

                config = {"hosts": [("127.0.0.1", 3000)]}
                client = aerospike.client(config).connect()

                try:
                    unique_id = 1
                    key = ("test", "demo", unique_id)
                    client.put(key, {"name": "John", "charges": [10, 20, 14]})

                    ops = [list_operations.list_append("charges", 25)]

                    # check that the record has value 'Kim' in bin 'name'
                    expr = exp.Eq(exp.StrBin("name"), "Kim").compile()

                    # Because the record's name bin is 'John' and not 'Kim',
                    # client.operate() will fail with AEROSPIKE_FILTERED_OUT and the
                    # operations will not be applied.
                    try:
                        client.operate(key, ops, policy={"expressions": expr})
                    except ex.FilteredOut as e:
                        print("Error: {0} [{1}]".format(e.msg, e.code))

                    record = client.get(key)
                    print(record)

                    # This client.operate() will succeed because the name bin is 'John'.
                    # check that the record has value 'John' in bin 'name'
                    expr = exp.Eq(exp.StrBin("name"), "John").compile()

                    client.operate(key, ops, policy={"expressions": expr})

                    record = client.get(key)
                    print(record)

                except ex.AerospikeError as e:
                    print("Error: {0} [{1}]".format(e.msg, e.code))
                    sys.exit(1)
                finally:
                    client.close()
                # Error: 127.0.0.1:3000 AEROSPIKE_FILTERED_OUT [27]
                # (('test', 'demo', None, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'ttl': 2592000, 'gen': 23}, {'number': 1, 'name': 'John', 'charges': [10, 20, 14]})
                # (('test', 'demo', None, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'ttl': 2592000, 'gen': 24}, {'number': 1, 'name': 'John', 'charges': [10, 20, 14, 25]})

        .. note::
            In version `2.1.3` the return format of certain bin entries for this method, **only in cases when a map operation specifying a** `return_type` **is used**, has changed. Bin entries for map operations using "return_type" of aerospike.MAP_RETURN_KEY_VALUE will now return \
            a bin value of a list of keys and corresponding values, rather than a list of key/value tuples. See the following code block for details.

        .. code-block:: python

            # pre 2.1.3 formatting of key/value bin value
            [('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')]

            # >= 2.1.3 formatting
            ['key1', 'val1', 'key2', 'val2', 'key3', 'val3']

        .. note::

            :meth:`operate` can now have multiple write operations on a single
            bin.

        .. note::

            :const:`~aerospike.OPERATOR_TOUCH` should only ever combine with
            :const:`~aerospike.OPERATOR_READ`, for example to implement LRU
            expiry on the records of a set.

        .. warning::

            Having *val* associated with :const:`~aerospike.OPERATOR_TOUCH` is deprecated.
            Use the meta *ttl* field instead.

        .. code-block:: python

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

    .. method:: operate_ordered(key, operations: list[, meta: dict[, policy: dict]]) -> (key, meta, bins)

        Performs an atomic transaction, with multiple bin operations, against a single record with a given *key*. \
        The results will be returned as a list of (bin-name, result) tuples. The order of the \
        elements in the list will correspond to the order of the operations \
        from the input parameters.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list operations: a :class:`list` of one or more bin operations, each \
            structured as the :class:`dict` \
            ``{'bin': bin name, 'op': aerospike.OPERATOR_* [, 'val': value]}``. \
            See :ref:`aerospike_operation_helpers.operations`.
        :param dict meta: optional record metadata to be set, with field
            ``'ttl'`` set to :class:`int` number of seconds or one of the :ref:`TTL_CONSTANTS`, \
            and ``'gen'`` set to :class:`int` generation number to compare.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`. See :ref:`unicode_handling`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note::
            In version `2.1.3` the return format of bin entries for this method, **only in cases when a map operation specifying a** `return_type` **is used**, has changed. Map operations using "return_type" of aerospike.MAP_RETURN_KEY_VALUE will now return \
            a bin value of a list of keys and corresponding values, rather than a list of key/value tuples. See the following code block for details. In addition, some operations which did not return a value in versions <= 2.1.2 will now return a response.

        .. code-block:: python

            # pre 2.1.3 formatting of key/value bin value
            [('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')]

            # >= 2.1.3 formatting
            ['key1', 'val1', 'key2', 'val2', 'key3', 'val3']

        .. code-block:: python

            import aerospike
            from aerospike import exception as ex
            from aerospike_helpers.operations import operations as op_helpers
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

                llist = [
                    op_helpers.append("name", "aa"),
                    op_helpers.read("name"),
                    op_helpers.increment("age", 3),
                    op_helpers.read("age")
                ]

                client.operate_ordered(key, llist, {}, policy)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()

        .. versionchanged:: 2.1.3

    .. index::
        single: User Defined Functions

    .. _aerospike_udf_operations:

User Defined Functions
----------------------

.. class:: Client
    :noindex:

    .. method:: udf_put(filename[, udf_type=aerospike.UDF_TYPE_LUA[, policy: dict]])

        Register a UDF module with the cluster.

        :param str filename: the path to the UDF module to be registered with the cluster.
        :param int udf_type: :data:`aerospike.UDF_TYPE_LUA`.
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. note::
        To run this example, do not run the boilerplate code.

    .. code-block:: python
        :emphasize-lines: 5,9

        import aerospike

        config = {
            'hosts': [ ('127.0.0.1', 3000)],
            'lua': { 'user_path': '/path/to/lua/user_path'}
        }
        client = aerospike.client(config).connect()
        # Register the UDF module and copy it to the Lua 'user_path'
        client.udf_put('/path/to/my_module.lua')
        client.close()

    .. method:: udf_remove(module[, policy: dict])

        Remove a  previously registered UDF module from the cluster.

        :param str module: the UDF module to be deregistered from the cluster.
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            client.udf_remove('my_module.lua')

    .. method:: udf_list([policy: dict]) -> []

        Return the list of UDF modules registered with the cluster.

        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`list`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            print(client.udf_list())
            # [
            #    {'content': bytearray(b''),
            #    'hash': bytearray(b'195e39ceb51c110950bd'),
            #    'name': 'my_udf1.lua',
            #    'type': 0},
            #    {'content': bytearray(b''),
            #    'hash': bytearray(b'8a2528e8475271877b3b'),
            #    'name': 'stream_udf.lua',
            #    'type': 0}
            # ]

    .. method:: udf_get(module[, language=aerospike.UDF_TYPE_LUA[, policy: dict]]) -> str

        Return the content of a UDF module which is registered with the cluster.

        :param str module: the UDF module to read from the cluster.
        :param int udf_type: :data:`aerospike.UDF_TYPE_LUA`
        :param dict policy: currently **timeout** in milliseconds is the available policy.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: apply(key, module, function, args[, policy: dict])

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

    .. note::
        Version >= 5.0.0 Supports aerrospike expressions for apply, scan_apply, and query_apply see :ref:`aerospike_operation_helpers.expressions`.
        Requires server version >= 5.2.0.

        .. code-block:: python

            import aerospike
            from aerospike_helpers import expressions as exp
            from aerospike import exception as ex
            import sys

            config = {"hosts": [("127.0.0.1", 3000)]}
            client = aerospike.client(config).connect()

            # register udf
            try:
                client.udf_put("/path/to/my_udf.lua")
            except ex.FilteredOut as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                client.close()
                sys.exit(1)


            # put records and apply udf
            try:
                keys = [("test", "demo", 1), ("test", "demo", 2), ("test", "demo", 3)]
                records = [{"number": 1}, {"number": 2}, {"number": 3}]
                for i in range(3):
                    client.put(keys[i], records[i])

                # check that the record has value < 2 or == 3 in bin 'number'
                expr = exp.Or(
                    exp.LT(exp.IntBin("number"), 2),
                    exp.Eq(exp.IntBin("number"), 3)
                ).compile()

                policy = {"expressions": expr}

                client.scan_apply("test", None, "my_udf", "my_udf", ["number", 10], policy)
                records = client.get_many(keys)

                print(records)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()
            # the udf has only modified the records that matched the preds
            # EXPECTED OUTPUT:
            # [
            #   (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 2, 'ttl': 2591999}, {'number': 11}),
            #   (('test', 'demo', 2, bytearray(b'\xaejQ_7\xdeJ\xda\xccD\x96\xe2\xda\x1f\xea\x84\x8c:\x92p')), {'gen': 12, 'ttl': 2591999}, {'number': 2}),
            #   (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 13, 'ttl': 2591999}, {'number': 13})
            # ]
        
        .. code-block:: python

            # contents of my_udf.lua
            function my_udf(rec, bin, offset)
                info("my transform: %s", tostring(record.digest(rec)))
                rec[bin] = rec[bin] + offset
                aerospike:update(rec)
            end

    .. method:: scan_apply(ns, set, module, function[, args[, policy: dict[, options]]]) -> int

        .. deprecated:: 7.0.0 :class:`aerospike.Query` should be used instead.

        Initiate a scan and apply a record UDF to each record matched by the scan. This method blocks until the scan is complete.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be :py:obj:`None` if the entire namespace is to be scanned.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the scan.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_scan_policies`.
        :param dict options: the :ref:`aerospike_scan_options` that will apply to the scan.
        :rtype: :class:`int`
        :return: a job ID that can be used with :meth:`job_info` to check the status of the ``aerospike.JOB_SCAN``.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Record UDF <http://www.aerospike.com/docs/guide/record_udf.html>`_ \
          and `Developing Record UDFs <http://www.aerospike.com/docs/udf/developing_record_udfs.html>`_.


    .. method:: query_apply(ns, set, predicate, module, function[, args[, policy: dict]]) -> int

        Initiate a query and apply a record UDF to each record matched by the query. This method blocks until the query is completed.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name. Should be :py:obj:`None` if you want to query records in the *ns* which are in no set.
        :param tuple predicate: the `tuple` produced by one of the :mod:`aerospike.predicates` methods.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the records matched by the query.
        :param list args: the arguments to the UDF.
        :param dict policy: optional :ref:`aerospike_write_policies`.
        :rtype: :class:`int`
        :return: a job ID that can be used with :meth:`job_info` to check the status of the ``aerospike.JOB_QUERY``.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `Record UDF <http://www.aerospike.com/docs/guide/record_udf.html>`_ \
          and `Developing Record UDFs <http://www.aerospike.com/docs/udf/developing_record_udfs.html>`_.


    .. method:: job_info(job_id, module[, policy: dict]) -> dict

        Return the status of a job running in the background.

        :param int job_id: the job ID returned by :meth:`scan_apply` and :meth:`query_apply`.
        :param module: one of :ref:`aerospike_job_constants`.
        :returns: a :class:`dict` with keys *status*, *records_read*, and \
          *progress_pct*. The value of *status* is one of :ref:`aerospike_job_constants_status`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

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


    .. method:: scan_info(scan_id) -> dict

        .. deprecated:: 1.0.50
            Use :meth:`job_info` instead.

        Return the status of a scan running in the background.

        :param int scan_id: the scan ID returned by :meth:`scan_apply`.
        :returns: a :class:`dict` with keys *status*, *records_scanned*, and \
          *progress_pct*. The value of *status* is one of :ref:`aerospike_job_constants_status`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

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

Info Operations
---------------

.. class:: Client
    :noindex:

    .. method:: get_node_names() -> []

        Return the list of hosts and node names present in a connected cluster.

        :return: a :class:`list` of node info dictionaries.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            # Assuming two nodes
            nodes = client.get_node_names()
            print(nodes)
            # [{'address': '1.1.1.1', 'port': 3000, 'node_name': 'BCER199932C'}, {'address': '1.1.1.1', 'port': 3010, 'node_name': 'ADFFE7782CD'}]

        .. versionchanged:: 6.0.0

    .. method:: get_nodes() -> []

        Return the list of hosts present in a connected cluster.

        :return: a :class:`list` of node address tuples.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            # Assuming two nodes
            nodes = client.get_nodes()
            print(nodes)
            # [('127.0.0.1', 3000), ('127.0.0.1', 3010)]

        .. versionchanged:: 3.0.0

        .. warning:: In versions < 3.0.0 ``get_nodes`` will not work when using TLS

    .. method:: info_single_node(command, host[, policy: dict]) -> str

        Send an info *command* to a single node specified by *host name*.

        :param str command: the info command. See `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.
        :param str host: a node name. Example: 'BCER199932C'
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Use :meth:`get_node_names` as an easy way to get host IP to node name mappings.

    .. method:: info_all(command[, policy: dict]]) -> {}

        Send an info command to all nodes in the cluster to which the client is connected. 
        
        If any of the individual requests fail, this will raise an exception.

        :param str command: see `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`dict`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            response = client.info_all("namespaces")
            print(response)
            # {'BB9020011AC4202': (None, 'test\n')}

        .. versionadded:: 3.0.0

    .. method:: info_random_node(command, [policy: dict]) -> str

        Send an info *command* to a single random node.

        :param str command: the info command. See `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 6.0.0

    .. method:: info_node(command, host[, policy: dict]) -> str

        .. deprecated:: 6.0.0
            Use :meth:`info_single_node` to send a request to a single node.

        Send an info command to a single node specified by host.

        :param str command: the info command. See `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_.
        :param tuple host: a `tuple` containing an *address*, *port* , optional *tls-name* . Example: ``('127.0.0.1', 3000)`` or when using TLS ``('127.0.0.1', 4333, 'server-tls-name')``. In order to send an info request when TLS is enabled, the *tls-name* must be present.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`str`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. versionchanged:: 3.0.0

        .. warning:: for client versions < 3.0.0 :meth:`~aerospike.client.info_node` will not work when using TLS

    .. method:: info(command[, hosts[, policy: dict]]) -> {}

        .. deprecated:: 3.0.0
            Use :meth:`info_all` to send a request to the entire cluster.

        Send an info command to all nodes in the cluster, and optionally filter responses to only include certain nodes.

        :param str command: the info command. See `Info Command Reference <http://www.aerospike.com/docs/reference/info/>`_
        :param list hosts: a :class:`list` containing ``(address, port)`` tuples. If specified, only send the command to these hosts. Example: ``[('127.0.0.1', 3000)]``
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :rtype: :class:`dict`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. code-block:: python

            response = client.info(command)
            client.close()
            # {'BB9581F41290C00': (None, '127.0.0.1:3000\n'), 'BC3581F41290C00': (None, '127.0.0.1:3010\n')}

        .. note::
                Sending requests to specific nodes can be better handled with a simple Python function such as:

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

        .. versionchanged:: 3.0.0

    .. method:: set_xdr_filter(data_center, namespace, expression_filter[, policy: dict]) -> str

        Set the cluster's xdr filter using an Aerospike expression.

        The cluster's current filter can be removed by setting expression_filter to None.

        :param str data_center: The data center to apply the filter to.
        :param str namespace: The namespace to apply the filter to.
        :param AerospikeExpression expression_filter: The filter to set. See expressions at :py:mod:`aerospike_helpers`.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `xdr-set-filter Info Command Reference <http://www.aerospike.com/docs/reference/info/#xdr-set-filter>`_.

        .. versionchanged:: 5.0.0

        .. warning:: Requires Aerospike server version >= 5.3.

    .. method:: get_expression_base64(expression) -> str

        Get the base64 representation of a compiled aerospike expression.

        See :ref:`aerospike_operation_helpers.expressions` for more details on expressions.

        :param AerospikeExpression expression: the compiled expression.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/get_expression_base64.py
            :code: python

        .. versionchanged:: 7.0.0

    .. method:: shm_key()  ->  int

        Expose the value of the shm_key for this client if shared-memory cluster tending is enabled, 

        :rtype: :class:`int` or :py:obj:`None`

    .. method:: truncate(namespace, set, nanos[, policy: dict])
        
        Remove all records in the namespace / set whose last updated time is older than the given time.
        
        This method is many orders of magnitude faster than deleting records one at a time. 
        See `Truncate command reference <https://www.aerospike.com/docs/reference/info#truncate>`_.
 
        This asynchronous server call may return before the truncation is complete.  The user can still
        write new records after the server returns because new records will have last update times
        greater than the truncate cutoff (set at the time of truncate call)

        :param str namespace: The namespace to truncate.
        :param str set: The set to truncate. Pass in :py:obj:`None` to truncate a namespace instead.
        :param long nanos:  A cutoff threshold where records last updated before the threshold will be removed.
            Units are in nanoseconds since the UNIX epoch ``(1970-01-01)``.
            A value of ``0`` indicates that all records in the set should be truncated regardless of update time.
            The value must not be in the future.
        :param dict policy: See :ref:`aerospike_info_policies`.
        :rtype: Status indicating the success of the operation.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires Aerospike server version >= 3.12

        .. include:: examples/truncate.py
            :code: python

    .. index::
        single: Index Operations

Index Operations
----------------

.. class:: Client
    :noindex:

    .. method:: index_string_create(ns, set, bin, index_name[, policy: dict])

        Create a string index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: index_integer_create(ns, set, bin, index_name[, policy])

        Create an integer index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: index_list_create(ns, set, bin, index_datatype, index_name[, policy: dict])

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

    .. method:: index_map_keys_create(ns, set, bin, index_datatype, index_name[, policy: dict])

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

    .. method:: index_map_values_create(ns, set, bin, index_datatype, index_name[, policy: dict])

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

    .. method:: index_geo2dsphere_create(ns, set, bin, index_name[, policy: dict])

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


    .. method:: index_remove(ns, index_name[, policy: dict])

        Remove the index with *index_name* from the namespace.

        :param str ns: the namespace in the aerospike cluster.
        :param str index_name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.


    .. index::
        single: Admin Operations

Admin Operations
----------------

.. class:: Client
    :noindex:

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


    .. method:: admin_create_role(role, privileges[, policy: dict[, whitelist[, read_quota[, write_quota]]]])

        Create a custom, named *role* containing a :class:`list` of *privileges*, optional whitelist, and quotas.

        :param str role: The name of the role.
        :param list privileges: A list of :ref:`aerospike_privilege_dict`.
        :param dict policy: Optional :ref:`aerospike_admin_policies`.
        :param list whitelist: A list of whitelist IP addresses that can contain wildcards, for example 10.1.2.0/24.
        :param int read_quota: Maximum reads per second limit, pass in zero for no limit.
        :param int write_quota: Maximum write per second limit, pass in zero for no limit.
        :raises: One of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_whitelist(role, whitelist[, policy: dict])

        Add *whitelist* to a *role*.

        :param str role: The name of the role.
        :param list whitelist: List of IP strings the role is allowed to connect to.
            Setting whitlist to None will clear the whitelist for that role.
        :param dict policy: Optional :ref:`aerospike_admin_policies`.
        :raises: One of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_quotas(role[, read_quota[, write_quota[, policy: dict]]])

        Add *quotas* to a *role*.

        :param str role: the name of the role.
        :param int read_quota: Maximum reads per second limit, pass in zero for no limit.
        :param int write_quota: Maximum write per second limit, pass in zero for no limit.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_drop_role(role[, policy: dict])

        Drop a custom *role*.

        :param str role: the name of the role.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_grant_privileges(role, privileges[, policy: dict])

        Add *privileges* to a *role*.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_revoke_privileges(role, privileges[, policy: dict])

        Remove *privileges* from a *role*.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: optional :`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_get_role(role[, policy: dict]) -> []

        Get the :class:`dict` of privileges, whitelist, and quotas associated with a *role*.

        :param str role: the name of the role.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :ref:`aerospike_privilege_dict`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_get_roles([policy: dict]) -> {}

        Get all named roles and their attributes.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of :ref:`aerospike_privilege_dict` keyed by role name.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_role(role[, policy: dict]) -> []

        Get the :class:`list` of privileges associated with a *role*.

        :param str role: the name of the role.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`list` of :ref:`aerospike_privilege_dict`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_roles([policy: dict]) -> {}

        Get all named roles and their privileges.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of :ref:`aerospike_privilege_dict` keyed by role name.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_create_user(username, password, roles[, policy: dict])

        Create a user with a specified *username* and grant it *roles*.

        :param str username: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param list roles: the list of role names assigned to the user.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_drop_user(username[, policy: dict])

        Drop the user with a specified *username* from the cluster.

        :param str username: the username to be dropped from the aerospike cluster.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_change_password(username, password[, policy: dict])

        Change the *password* of the user *username*. This operation can only \
        be performed by that same user.

        :param str username: the username.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_password(username, password[, policy: dict])

        Set the *password* of the user *username* by a user administrator.

        :param str username: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_grant_roles(username, roles[, policy: dict])

        Add *roles* to the user *username*.

        :param str username: the username to be granted the roles.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_revoke_roles(username, roles[, policy: dict])

        Remove *roles* from the user *username*.

        :param str username: the username to have the roles revoked.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_user (username[, policy: dict]) -> []

        Return the list of roles granted to the specified user *username*.

        :param str username: the username to query for.
        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`list` of role names.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_users ([policy: dict]) -> {}

        Return the :class:`dict` of users, with their roles keyed by username.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of roles keyed by username.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

Scan and Query Constructors
---------------------------

.. class:: Client
    :noindex:

    .. method:: scan(namespace[, set]) -> Scan

        .. deprecated:: 7.0.0 :class:`aerospike.Query` should be used instead.

        Returns a :class:`aerospike.Scan` object to scan all records in a namespace / set.
        
        If set is omitted or set to :py:obj:`None`, the object returns all records in the namespace.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the entire \
            *namespace* will be scanned.

        :return: an :py:class:`aerospike.Scan` class.

    .. method:: query(namespace[, set]) -> Query

        Return a :class:`aerospike.Query` object to be used for executing queries
        over a specified set in a namespace.
        
        (which can be omitted or :py:obj:`None`)
        If the set is omitted or set to :py:obj:`None`, the object returns records **not in any \
        named set**. This is different than the meaning of a :py:obj:`None` set in \
        a scan.

        :param str namespace: the namespace in the aerospike cluster.
        :param str set: optional specified set name, otherwise the records \
            which are not part of any *set* will be queried (**Note**: this is \
            different from not providing the *set* in :meth:`scan`).
        :return: an :py:class:`aerospike.Query` class.

.. index::
    single: Other Methods

Tuples
======

.. _aerospike_key_tuple:

Key Tuple
---------

    .. object:: key

        The key tuple, which is sent and returned by various operations, has the structure

        ``(namespace, set, primary key[, digest])``

        .. hlist::
            :columns: 1

            * namespace (:class:`str`)
                Name of the namespace.

                This must be preconfigured on the cluster.

            * set (:class:`str`)
                Name of the set.

                The set be created automatically if it does not exist.

            * primary key (:class:`str`, :class:`int` or :class:`bytearray`)
                The value by which the client-side application identifies the record.

            * digest
                The record's RIPEMD-160 digest.

                The first three parts of the tuple get hashed through RIPEMD-160, \
                and the digest used by the clients and cluster nodes to locate the record. \
                A key tuple is also valid if it has the digest part filled and the primary key part set to :py:obj:`None`.

    The following code example shows:
        
    * How to use the key tuple in a `put` operation
    * How to fetch the key tuple in a `get` operation

    .. code-block:: python

        import aerospike

        # NOTE: change this to your Aerospike server's seed node address
        seedNode = ('127.0.0.1', 3000)
        config = config = {'hosts': [seedNode]}
        client = aerospike.client(config).connect()

        # The key tuple comprises the following:
        namespaceName = 'test'
        setName = 'setname'
        primaryKeyName = 'pkname'
        keyTuple = (namespaceName, setName, primaryKeyName)

        # Insert a record
        recordBins = {'bin1':0, 'bin2':1}
        client.put(keyTuple, recordBins)

        # Now fetch that record
        (key, meta, bins) = client.get(keyTuple)

        # The key should be in the second format
        # Notice how there is no primary key
        # and there is the record's digest
        print(key)

        # Expected output: 
        # ('test', 'setname', None, bytearray(b'b\xc7[\xbb\xa4K\xe2\x9al\xd12!&\xbf<\xd9\xf9\x1bPo'))

        # Cleanup
        client.remove(keyTuple)
        client.close()

    .. seealso:: `Data Model: Keys and Digests <https://www.aerospike.com/docs/architecture/data-model.html#records>`_.

.. _aerospike_record_tuple:

Record Tuple
------------

.. object:: record

    The record tuple which is returned by various read operations. It has the structure:

    ``(key, meta, bins)``

    .. hlist::
        :columns: 1

        * key (:class:`tuple`)
            See :ref:`aerospike_key_tuple`.
        * **meta** (:class:`dict`)
            Contains record metadata with the following key-value pairs:

            * **gen** (:class:`int`)
                Generation value

            * **ttl** (:class:`int`)
                Time-to-live value

        * bins (:class:`dict`)
            Contains bin-name/bin-value pairs.

    We reuse the code example in the key-tuple section and print the ``meta`` and ``bins`` values that were returned from ``client.get()``:

        .. code-block:: python

            import aerospike

            # NOTE: change this to your Aerospike server's seed node address
            seedNode = ('127.0.0.1', 3000)
            config = {'hosts': [seedNode]}
            client = aerospike.client(config).connect()

            namespaceName = 'test'
            setName = 'setname'
            primaryKeyName = 'pkname'
            keyTuple = (namespaceName, setName, primaryKeyName)

            # Insert a record
            recordBins = {'bin1':0, 'bin2':1}
            client.put(keyTuple, recordBins)

            # Now fetch that record
            (key, meta, bins) = client.get(keyTuple)

            # Generation is 1 because this is the first time we wrote the record
            print(meta)

            # Expected output:
            # {'ttl': 2592000, 'gen': 1}

            # The bin-value pairs we inserted
            print(bins)
            {'bin1': 0, 'bin2': 1}

            client.remove(keyTuple)
            client.close()

    .. seealso:: `Data Model: Record <https://www.aerospike.com/docs/architecture/data-model.html#records>`_.

.. _metadata_dict:

Metadata Dictionary
===================

The metadata dictionary has the following key-value pairs:

    * ``"ttl"`` (:class:`int`): record time to live in seconds. See :ref:`TTL_CONSTANTS`.
    * ``"gen"`` (:class:`int`): record generation

.. _aerospike_polices:

Policies
========

.. _aerospike_write_policies:

Write Policies
--------------

.. object:: policy

    A :class:`dict` of optional write policies, which are applicable to :meth:`~Client.put`, :meth:`~Client.query_apply`. :meth:`~Client.remove_bin`.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``0``
            
            .. warning:: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times \
               if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;
            
        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep. 
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. \
              If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **exists** 
            | One of the :ref:`POLICY_EXISTS` values such as :data:`aerospike.POLICY_EXISTS_CREATE`
            |
            | Default: :data:`aerospike.POLICY_EXISTS_IGNORE`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            |
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.
        * **compression_threshold** (:class:`int`)
            Compress data for transmission if the object size is greater than a given number of bytes.

            Default: ``0``, meaning 'never compress'

.. _aerospike_read_policies:

Read Policies
-------------

.. object:: policy

    A :class:`dict` of optional read policies, which are applicable to :meth:`~Client.get`, :meth:`~Client.exists`, :meth:`~Client.select`.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``2``
        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep. 
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **deserialize** (:class:`bool`)
            | Should raw bytes representing a list or map be deserialized to a list or dictionary.
            | Set to `False` for backup programs that just need access to raw bytes.
            | Default: ``True``
        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **read_mode_ap** 
            | One of the :ref:`POLICY_READ_MODE_AP` values such as :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
            |
            | Default: :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`

            .. versionadded:: 3.7.0

        * **read_mode_sc** 
            | One of the :ref:`POLICY_READ_MODE_SC` values such as :data:`aerospike.POLICY_READ_MODE_SC_SESSION`
            |
            | Default: :data:`aerospike.POLICY_READ_MODE_SC_SESSION`

            .. versionadded:: 3.7.0

        * **replica** 
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            |
            | Default: ``aerospike.POLICY_REPLICA_SEQUENCE``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

.. _aerospike_operate_policies:

Operate Policies
----------------

.. object:: policy

    A :class:`dict` of optional operate policies, which are applicable to :meth:`~Client.append`, :meth:`~Client.prepend`, :meth:`~Client.increment`, :meth:`~Client.operate`, and atomic list and map operations.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``0``

            .. warning::  Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times \
               if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;

        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            |
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **replica** 
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            |
            | Default: :data:`aerospike.POLICY_REPLICA_SEQUENCE`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **read_mode_ap** 
            | One of the :ref:`POLICY_READ_MODE_AP` values such as :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
            | 
            | Default: :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`

            .. versionadded:: 3.7.0

        * **read_mode_sc** 
            | One of the :ref:`POLICY_READ_MODE_SC` values such as :data:`aerospike.POLICY_READ_MODE_SC_SESSION`
            |
            | Default: :data:`aerospike.POLICY_READ_MODE_SC_SESSION`

            .. versionadded:: 3.7.0

        * **exists** 
            | One of the :ref:`POLICY_EXISTS` values such as :data:`aerospike.POLICY_EXISTS_CREATE`
            |
            | Default: :data:`aerospike.POLICY_EXISTS_IGNORE`
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

.. _aerospike_apply_policies:

Apply Policies
--------------

.. object:: policy

    A :class:`dict` of optional apply policies, which are applicable to :meth:`~Client.apply`.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``0``
            
            .. warning::  Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times \
               if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;

        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep. 
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **replica** 
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            |
            | Default: :data:`aerospike.POLICY_REPLICA_SEQUENCE`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            | 
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None
            
            .. note:: Requires Aerospike server version >= 5.2.


.. _aerospike_remove_policies:

Remove Policies
---------------

.. object:: policy

    A :class:`dict` of optional remove policies, which are applicable to :meth:`~Client.remove`.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``0``

            .. warning::  Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times \
               if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;

        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep. 
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            | 
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            | 
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``

            .. note:: Requires Enterprise server version >= 3.10

        * **replica** 
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            | 
            | Default: ``aerospike.POLICY_REPLICA_SEQUENCE``

        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

.. _aerospike_batch_policies:

Batch Policies
--------------

.. object:: policy

    A :class:`dict` of optional batch policies, which are applicable to :meth:`~aerospike.get_many`, :meth:`~aerospike.exists_many` and :meth:`~aerospike.select_many`.

    .. hlist::
        :columns: 1

        * **max_retries** (:class:`int`)
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
            |
            | Default: ``2``

            .. warning: Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed multiple times if the client timed out previous  transaction attempts. It's important to use a distinct write policy for non-idempotent writes, which sets max_retries = `0`;

        * **sleep_between_retries** (:class:`int`)
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout** (:class:`int`)
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``0``
        * **total_timeout** (:class:`int`)
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``1000``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **read_mode_ap** 
            | One of the :ref:`POLICY_READ_MODE_AP` values such as :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
            | 
            | Default: :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
            
            .. versionadded:: 3.7.0
        
        * **read_mode_sc** 
            | One of the :ref:`POLICY_READ_MODE_SC` values such as :data:`aerospike.POLICY_READ_MODE_SC_SESSION`
            | 
            | Default: :data:`aerospike.POLICY_READ_MODE_SC_SESSION`

            .. versionadded:: 3.7.0

        * **replica** 
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            | 
            | Default: :data:`aerospike.POLICY_REPLICA_SEQUENCE`
        * **concurrent** (:class:`bool`)
            | Determine if batch commands to each server are run in parallel threads. 
            | 
            | Default ``False``
        * **allow_inline** (:class:`bool`)
            | Allow batch to be processed immediately in the server's receiving thread when the server deems it to be appropriate.  If `False`, the batch will always be processed in separate transaction threads.  This field is only relevant for the new batch index protocol. 
            | 
            | Default ``True``
        * **send_set_name** (:class:`bool`) 
            |
            |   .. deprecated:: in client version 7.0.0, the client ignores this policy and always sends set name to the server.
            |
            | Send set name field to server for every key in the batch for batch index protocol. This is only necessary when authentication is enabled and security roles are defined on a per set basis. 
            | 
            | Default: ``False``
        * **deserialize** (:class:`bool`) 
            | Should raw bytes be deserialized to as_list or as_map. Set to `False` for backup programs that just need access to raw bytes. 
            | 
            | Default: ``True``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

.. _aerospike_batch_write_policies:

Batch Write Policies
--------------------

.. object:: policy

    A :class:`dict` of optional batch write policies, which are applicable to :meth:`~aerospike.batch_write`, :meth:`~aerospike.batch_operate` and :class:`Write <aerospike_helpers.batch.records>`.

    .. hlist::
        :columns: 1

        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            |
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **exists** 
            | One of the :ref:`POLICY_EXISTS` values such as :data:`aerospike.POLICY_EXISTS_CREATE`
            |
            | Default: :data:`aerospike.POLICY_EXISTS_IGNORE`
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

.. _aerospike_batch_apply_policies:

Batch Apply Policies
--------------------

.. object:: policy

    A :class:`dict` of optional batch apply policies, which are applicable to :meth:`~aerospike.batch_apply`, and :class:`Apply <aerospike_helpers.batch.records>`.

    .. hlist::
        :columns: 1

        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **ttl** int
            | Time to live (expiration) of the record in seconds.
            |
            | 0 which means that the
            | record will adopt the default TTL value from the namespace.
            |
            | 0xFFFFFFFF (also, -1 in a signed 32 bit int)
            | which means that the record
            | will get an internal "void_time" of zero, and thus will never expire.
            |
            | 0xFFFFFFFE (also, -2 in a signed 32 bit int)
            | which means that the record
            |
            | ttl will not change when the record is updated.
            | Note that the TTL value will be employed ONLY on write/update calls.
            |
            | Default: 0
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

.. _aerospike_batch_remove_policies:

Batch Remove Policies
---------------------

.. object:: policy

    A :class:`dict` of optional batch remove policies, which are applicable to :meth:`~aerospike.batch_remove`, and :class:`Remove <aerospike_helpers.batch.records>`.

    .. hlist::
        :columns: 1

        * **key** 
            | One of the :ref:`POLICY_KEY` values such as :data:`aerospike.POLICY_KEY_DIGEST`
            |
            | Default: :data:`aerospike.POLICY_KEY_DIGEST`
        * **commit_level** 
            | One of the :ref:`POLICY_COMMIT_LEVEL` values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
            |
            | Default: :data:`aerospike.POLICY_COMMIT_LEVEL_ALL`
        * **gen** 
            | One of the :ref:`POLICY_GEN` values such as :data:`aerospike.POLICY_GEN_IGNORE`
            |
            | Default: :data:`aerospike.POLICY_GEN_IGNORE`
        * **generation** int
            | Generation of the record.
            |
            | Default: 0
        * **durable_delete** (:class:`bool`)
            | Perform durable delete
            |
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

.. _aerospike_batch_read_policies:

Batch Read Policies
-------------------

.. object:: policy

    A :class:`dict` of optional batch read policies, which are applicable to :class:`Read <aerospike_helpers.batch.records>`.

    .. hlist::
        :columns: 1

        * **read_mode_ap** 
            | One of the :ref:`POLICY_READ_MODE_AP` values such as :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
            |
            | Default: :data:`aerospike.AS_POLICY_READ_MODE_AP_ONE`
        * **read_mode_sc** 
            | One of the :ref:`POLICY_READ_MODE_SC` values such as :data:`aerospike.POLICY_READ_MODE_SC_SESSION`
            |
            | Default: :data:`aerospike.POLICY_READ_MODE_SC_SESSION`
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

.. _aerospike_info_policies:

Info Policies
-------------

.. object:: policy
    
    A :class:`dict` of optional info policies, which are applicable to :meth:`~aerospike.info_all`, :meth:`~aerospike.info_single_node`, :meth:`~aerospike.info_random_node` and index operations.

    .. hlist::
        :columns: 1

        * **timeout** (:class:`int`)
            | Read timeout in milliseconds


.. _aerospike_admin_policies:

Admin Policies
--------------

.. object:: policy

    A :class:`dict` of optional admin policies, which are applicable to admin (security) operations.

    .. hlist::
        :columns: 1

        * **timeout** (:class:`int`)
            | Admin operation timeout in milliseconds


.. _aerospike_list_policies:

List Policies
-------------

.. object:: policy

    A :class:`dict` of optional list policies, which are applicable to list operations.


    .. hlist::
        :columns: 1

        * **write_flags** 
            | Write flags for the operation. 
            | One of the :ref:`aerospike_list_write_flag` values such as :data:`aerospike.LIST_WRITE_DEFAULT`
            | 
            | Default: :data:`aerospike.LIST_WRITE_DEFAULT`
            |
            | Values should be or'd together: 
            | ``aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_INSERT_BOUNDED``
            
        * **list_order** 
            | Ordering to maintain for the list. 
            | One of :ref:`aerospike_list_order`, such as :data:`aerospike.LIST_ORDERED` 
            |
            | Default: :data:`aerospike.LIST_UNORDERED`

    Example:

     .. code-block:: python

        list_policy = {
            "write_flags": aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_INSERT_BOUNDED,
            "list_order": aerospike.LIST_ORDERED
        }

.. _aerospike_map_policies:

Map Policies
------------

.. object:: policy

    A :class:`dict` of optional map policies, which are applicable to map operations. Only one of ``map_write_mode`` or ``map_write_flags`` should
    be provided. ``map_write_mode`` should be used for Aerospike Server versions < `4.3.0` and ``map_write_flags`` should be used for Aerospike server versions
    greater than or equal to `4.3.0` .

    .. hlist::
        :columns: 1

        * **map_write_mode**  
            | Write mode for the map operation. 
            | One of the :ref:`aerospike_map_write_mode` values such as :data:`aerospike.MAP_UPDATE`
            | 
            | Default: :data:`aerospike.MAP_UPDATE`

            .. note:: This should only be used for Server version < 4.3.0.
        
        * **map_write_flags**  
            | Write flags for the map operation. 
            | One of the :ref:`aerospike_map_write_flag` values such as :data:`aerospike.MAP_WRITE_FLAGS_DEFAULT`
            | 
            | Default: :data:`aerospike.MAP_WRITE_FLAGS_DEFAULT`
            |
            | Values should be or'd together: 
            | ``aerospike.LIST_WRITE_ADD_UNIQUE | aerospike.LIST_WRITE_INSERT_BOUNDED``

            .. note:: This is only valid for Aerospike Server versions >= 4.3.0. 
		
        * **map_order**  
            | Ordering to maintain for the map entries. 
            | One of :ref:`aerospike_map_order`, such as :data:`aerospike.MAP_KEY_ORDERED` 
            |
            | Default: :data:`aerospike.MAP_UNORDERED`
            
    Example:

    .. code-block:: python

        # Server >= 4.3.0
        map_policy = {
            'map_order': aerospike.MAP_UNORDERED,
            'map_write_flags': aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
        }

        # Server < 4.3.0
        map_policy = {
            'map_order': aerospike.MAP_UNORDERED,
            'map_write_mode': aerospike.MAP_CREATE_ONLY
        }

.. _aerospike_bit_policies:

Bit Policies
------------

.. object:: policy

    A :class:`dict` of optional bit policies, which are applicable to bitwise operations.

    .. note:: Requires server version >= 4.6.0

    .. hlist::
        :columns: 1

        * **bit_write_flags** 
            | Write flags for the bit operation. 
            | One of the :ref:`aerospike_bitwise_write_flag` values such as :data:`aerospike.BIT_WRITE_DEFAULT`
            | 
            | Default: :data:`aerospike.BIT_WRITE_DEFAULT`

    Example:

    .. code-block:: python

        bit_policy = {
            'bit_write_flags': aerospike.BIT_WRITE_UPDATE_ONLY
        }

.. _aerospike_hll_policies:

HyperLogLog Policies
--------------------

.. object:: policy

    A :class:`dict` of optional HyperLogLog policies, which are applicable to bit operations.

    .. note:: Requires server version >= 4.9.0

    .. hlist::
        :columns: 1

        * **flags** 
            | Write flags for the HLL operation. 
            | One of the :ref:`aerospike_hll_write_flags` values such as :data:`aerospike.HLL_WRITE_DEFAULT`
            | 
            | Default: :data:`aerospike.HLL_WRITE_DEFAULT`

    Example:

    .. code-block:: python

        HLL_policy = {
            'flags': aerospike.HLL_WRITE_UPDATE_ONLY
        }



Misc
====

.. _aerospike_role_dict:

Role Objects
------------

.. object:: Role Dictionary

    A :class:`dict` describing attributes associated with a specific role.

    .. hlist::
        :columns: 1

        * **privileges** A list of :ref:`aerospike_privilege_dict`.
        * **whitelist** A :class:`list` of IP address strings.
        * **read_quota** A :class:`int` representing the allowed read transactions per second.
        * **write_quota** A :class:`int` representing the allowed write transactions per second.

    Example:

    ``{'code': aerospike.PRIV_READ, 'ns': 'test', 'set': 'demo'}``

.. _aerospike_privilege_dict:

Privilege Objects
-----------------

.. object:: privilege

    A :class:`dict` describing a privilege associated with a specific role.

    .. hlist::
        :columns: 1

        * **code** one of the :ref:`aerospike_privileges` values such as :data:`aerospike.PRIV_READ`
        * **ns** optional namespace, to which the privilege applies, otherwise the privilege applies globally.
        * **set** optional set within the *ns*, to which the privilege applies, otherwise to the entire namespace.

    Example:

    ``{'code': aerospike.PRIV_READ, 'ns': 'test', 'set': 'demo'}``


.. _aerospike_partition_objects:

Partition Objects
-----------------

.. object:: partition_filter

    A :class:`dict` of partition information used by the client
    to perform partiton queries/scans. Useful for resuming terminated queries and
    querying particular partitons/records.

    .. hlist::
        :columns: 1

        * **begin** Optional :class:`int` signifying which partition to start at. Default: 0 (the first partition)
        * **count** Optional :class:`int` signifying how many partitions to process. Default: 4096 (all partitions)
        * **digest** Optional :class:`dict` containing the keys "init" and "value" signifying whether the digest has been calculated, and the digest value.
            |   **init**: :class:`bool` Whether the digest has been calculated.
            |   **value**: :class:`bytearray` The bytearray value of the digest, should be 20 characters long.
            |       ``# Example digest dict.``
            |       ``digest = {"init": True, "value": bytearray([0]*20)}``

            Default: {} (will start from first record in partition)
        * **partition_status** Optional :class:`dict` containing partition_status tuples. These can be used to resume a query/scan. Default: {} (all partitions)

    Default: ``{}`` (All partitions will be queried/scanned).

    .. code-block:: python

        # Example of a query policy using partition_filter.
        
        # partition_status is most easily used to resume a query
        # and can be obtained by calling Query.get_partitions_status()
        partition_status = {
            0: {0, False, Flase, bytearray([0]*20)}...
        }

        policy = {
            "partition_filter": {
                "partition_status": partition_status,
                "begin": 0,
                "count": 4096
            },
        }


.. object:: partition_status

    .. note:: Requires Aerospike server version >= 6.0.

    A :class:`dict` of partition status information used by the client
    to set the partition status of a query/scan during a partition query/scan.
    Useful for resuming partition query/scans.

    partition_status is a dictionary with keys "retry" `str`, "done" `str`, and a variable amount of id `int` keys.
    "retry" corresponds to the overall partition query retry status and maps to a bool. i.e. Does this query/scan need to be retried?
    "done" represents whether all partitions were finished and maps to a bool.
    the id keys, called "id" in this documentation correspond to a partition id. "id"'s value is
    another dictionary containing status details about that partition. See those values below.

    .. hlist::
        :columns: 1

        * **id** :class:`int` Represents the partition id number.
        * **init** :class:`bool` Represents whether the digest being queried was calculated.
        * **retry** :class:`bool` Represents whether this partition should be retried.
        * **digest** :class:`bytearray` Represents the digest of the record being queried. Should be 20 characters long.
        * **bval** :class:`int` Used in conjunction with digest in order to determine the last record recieved by a partition query.

    .. code-block:: python

       # Example of a query policy using partition_status.
       # Assume "query" is a valid aerospike Query instance.
       
       # partition_status is most easily used to resume a query
       # and can be obtained by calling Query.get_partitions_status()
       # Here is the form of partition_status.
       # partition_status = {
       #     0: (0, False, Flase, bytearray([0]*20), 0)...
       # }
       partition_status = query.get_partitions_status()

       policy = {
           "partition_filter": {
               "partition_status": partition_status,
               "begin": 0,
               "count": 4096
           },
       }

    Default: ``{}`` (All partitions will be queried).

.. _unicode_handling:

Unicode Handling
----------------

Both :class:`str` and `unicode` values are converted by the
client into UTF-8 encoded strings for storage on the aerospike server.
Read methods such as :meth:`~aerospike.get`,
:meth:`~aerospike.query`, :meth:`~aerospike.scan` and
:meth:`~aerospike.operate` will return that data as UTF-8 encoded
:class:`str` values. To get a `unicode` you will need to manually decode.

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
