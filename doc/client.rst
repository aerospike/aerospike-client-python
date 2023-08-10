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

.. warning::
    Only run example code on a brand new Aerospike server. This code deletes all records in the ``demo`` set!

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

To create a new client, use :meth:`aerospike.client`.

.. _aerospike_connection_operations:

Connection
----------

.. class:: Client

    .. method:: connect([username, password])

        If there is currently no connection to the cluster, connect to it. The optional *username* and *password* only
        apply when connecting to the Enterprise Edition of Aerospike.

        :param str username: a defined user with roles in the cluster. See :meth:`admin_create_user`.
        :param str password: the password will be hashed by the client using bcrypt.
        :raises: :exc:`~aerospike.exception.ClientError`, for example when a connection cannot be \
                 established to a seed node (any single node in the cluster from which the client \
                 learns of the other nodes).

        .. note::
            Python client 5.0.0 and up will fail to connect to Aerospike server 4.8.x or older.
            If you see the error "-10, ‘Failed to connect’", please make sure you are using server 4.9 or later.

        .. seealso:: `Security features article <https://docs.aerospike.com/server/guide/security/index.html>`_.

    .. method:: is_connected()

        Tests the connections between the client and the nodes of the cluster.
        If the result is ``False``, the client will require another call to
        :meth:`~aerospike.connect`.

        :rtype: :class:`bool`

        .. versionchanged:: 2.0.0

    .. method:: close()

        Close all connections to the cluster. It is recommended to explicitly \
        call this method when the program is done communicating with the cluster.

        You may call :meth:`~aerospike.Client.connect` again after closing the connection.

Record Operations
-----------------

.. class:: Client
    :noindex:

    .. method:: put(key, bins: dict[, meta: dict[, policy: dict[, serializer=aerospike.SERIALIZER_NONE]]])

        Create a new record, or remove / add bins to a record.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict bins: contains bin name-value pairs of the record.
        :param dict meta: record metadata to be set. see :ref:`metadata_dict`.
        :param dict policy: see :ref:`aerospike_write_policies`.

        :param serializer: override the serialization mode of the client \
            with one of the :ref:`aerospike_serialization_constants`.
            To use a class-level, user-defined serialization function registered with :func:`aerospike.set_serializer`, \
            use :const:`aerospike.SERIALIZER_USER`.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        Example:

        .. include:: examples/put.py
            :code: python

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

        :return: a :ref:`aerospike_record_tuple`.

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

        :return: a :ref:`aerospike_record_tuple`.

        :raises: :exc:`~aerospike.exception.RecordNotFound`.

        .. include:: examples/select.py
            :code: python

        .. versionchanged:: 2.0.0

    .. method:: touch(key[, val=0[, meta: dict[, policy: dict]]])

        Touch the given record, setting its time-to-live and incrementing its generation.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param int val: ttl in seconds, with ``0`` resolving to the default value in the server config.
        :param dict meta: record metadata to be set. see :ref:`metadata_dict`
        :param dict policy: see :ref:`aerospike_operate_policies`.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/touch.py
            :code: python

    .. method:: remove(key[meta: dict[, policy: dict]])

        Remove a record matching the *key* from the cluster.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param dict meta: contains the expected generation of the record in a key called ``"gen"``.
        :param dict policy: see :ref:`aerospike_remove_policies`. May be passed as a keyword argument.

        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/remove.py
            :code: python

    .. method:: remove_bin(key, list[, meta: dict[, policy: dict]])

        Remove a list of bins from a record with a given *key*. Equivalent to \
        setting those bins to :meth:`aerospike.null` with a :meth:`~aerospike.put`.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: the bins names to be removed from the record.
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
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

        .. deprecated:: 12.0.0
            Use :meth:`batch_read` instead.

    .. method:: exists_many(keys[, policy: dict]) -> [ (key, meta)]

        Batch-read metadata for multiple keys.

        Any record that does not exist will have a :py:obj:`None` value for metadata in \
        their tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple`.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of (key, metadata) :class:`tuple` for each record.

        .. include:: examples/exists_many.py
            :code: python

        .. deprecated:: 12.0.0
            Use :meth:`batch_read` instead.

    .. method:: select_many(keys, bins: list[, policy: dict]) -> [(key, meta, bins), ...]}

        Batch-read specific bins from multiple records.

        Any record that does not exist will have a :py:obj:`None` value for metadata and bins in its tuple.

        :param list keys: a list of :ref:`aerospike_key_tuple` to read from.
        :param list bins: a list of bin names to read from the records.
        :param dict policy: see :ref:`aerospike_batch_policies`.

        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. include:: examples/select_many.py
            :code: python

        .. deprecated:: 12.0.0
            Use :meth:`batch_read` instead.

    .. method:: batch_get_ops(keys, ops, policy: dict) -> [ (key, meta, bins)]

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

        .. deprecated:: 12.0.0
            Use :meth:`batch_operate` instead.

    The following batch methods will return a :class:`BatchRecords` object with
    a ``result`` value of ``0`` if one of the following is true:

        * All transactions are successful.
        * One or more transactions failed because:

            - A record was filtered out by an expression
            - The record was not found

    Otherwise if one or more transactions failed, the :class:`BatchRecords` object will have a ``result`` value equal to
    an `as_status <https://docs.aerospike.com/apidocs/c/dc/d42/as__status_8h.html>`_ error code.

    In any case, the :class:`BatchRecords` object has a list of batch records called ``batch_records``,
    and each batch record contains the result of that transaction.

    .. method:: batch_write(batch_records: BatchRecords, [policy_batch: dict]) -> BatchRecords

        Write/read multiple records for specified batch keys in one batch call.

        This method allows different sub-commands for each key in the batch.
        The resulting status and operated bins are set in ``batch_records.results`` and ``batch_records.record``.

        :param BatchRecords batch_records: A :class:`BatchRecords` object used to specify the operations to carry out.
        :param dict policy_batch: aerospike batch policy :ref:`aerospike_batch_policies`.

        :return: A reference to the batch_records argument of type :class:`BatchRecords <aerospike_helpers.batch.records>`.

        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_write.py
            :code: python

        .. note:: Requires server version >= 6.0.0.

        .. seealso:: More information about the \
            batch helpers :ref:`aerospike_operation_helpers.batch`

    .. method:: batch_read(keys: list, [bins: list], [policy_batch: dict]) -> BatchRecords

        Read multiple records.

        If a list of bin names is not provided, return all the bins for each record.

        If a list of bin names is provided, return only these bins for the given list of records.

        If an empty list of bin names is provided, only the metadata of each record will be returned.
        Each ``BatchRecord.record`` in ``BatchRecords.batch_records`` will only be a 2-tuple ``(key, meta)``.

        :param list keys: The key tuples of the records to fetch.
        :param list[str] bins: List of bin names to fetch for each record.
        :param dict policy_batch: See :ref:`aerospike_batch_policies`.

        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.

        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 6.0.0.

    .. method:: batch_operate(keys: list, ops: list, [policy_batch: dict], [policy_batch_write: dict]) -> BatchRecords

        Perform the same read/write transactions on multiple keys.

        :param list keys: The keys to operate on.
        :param list ops: List of operations to apply.
        :param dict policy_batch: See :ref:`aerospike_batch_policies`.
        :param dict policy_batch_write: See :ref:`aerospike_batch_write_policies`.

        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.

        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_operate.py
            :code: python

        .. note:: Requires server version >= 6.0.0.

    .. method:: batch_apply(keys: list, module: str, function: str, args: list, [policy_batch: dict], [policy_batch_apply: dict]) -> BatchRecords

        Apply UDF (user defined function) on multiple keys.

        :param list keys: The keys to operate on.
        :param str module: the name of the UDF module.
        :param str function: the name of the UDF to apply to the record identified by *key*.
        :param list args: the arguments to the UDF.
        :param dict policy_batch: See :ref:`aerospike_batch_policies`.
        :param dict policy_batch_apply: See :ref:`aerospike_batch_apply_policies`.

        :return: an instance of :class:`BatchRecords <aerospike_helpers.batch.records>`.
        :raises: A subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/batch_apply.py
            :code: python

        .. include:: examples/batch_apply.lua
            :code: lua

        .. note:: Requires server version >= 6.0.0.

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

        Append a string to the string value in bin.

        :param tuple key: a :ref:`aerospike_key_tuple` tuple associated with the record.
        :param str bin: the name of the bin.
        :param str val: the string to append to the bin value.
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
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
        :param str val: the string to prepend to the bin value.
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
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
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
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

    .. method:: operate(key, list: list[, meta: dict[, policy: dict]]) -> (key, meta, bins)

        Performs an atomic transaction, with multiple bin operations, against a single record with a given *key*.

        Starting with Aerospike server version 3.6.0, non-existent bins are not present in the returned :ref:`aerospike_record_tuple`. \
        The returned record tuple will only contain one element per bin, even if multiple operations were performed on the bin. \
        (In Aerospike server versions prior to 3.6.0, non-existent bins being read will have a \
        :py:obj:`None` value. )

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: See :ref:`aerospike_operation_helpers.operations`.
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
        :param dict policy: optional :ref:`aerospike_operate_policies`.
        :return: a :ref:`aerospike_record_tuple`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/operate.py
            :code: python

        .. note::

            :meth:`operate` can now have multiple write operations on a single
            bin.

        .. versionchanged:: 2.1.3

    .. method:: operate_ordered(key, list: list[, meta: dict[, policy: dict]]) -> (key, meta, bins)

        Performs an atomic transaction, with multiple bin operations, against a single record with a given *key*. \
        The results will be returned as a list of (bin-name, result) tuples. The order of the \
        elements in the list will correspond to the order of the operations \
        from the input parameters.

        Write operations or read operations that fail will not return a ``(bin-name, result)`` tuple.

        :param tuple key: a :ref:`aerospike_key_tuple` associated with the record.
        :param list list: See :ref:`aerospike_operation_helpers.operations`.
        :param dict meta: record metadata to be set. See :ref:`metadata_dict`.
        :param dict policy: optional :ref:`aerospike_operate_policies`.

        :return: a :ref:`aerospike_record_tuple`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/operate_ordered.py
            :code: python

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
        client = aerospike.client(config)
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

    .. method:: udf_get(module: str[, language: int = aerospike.UDF_TYPE_LUA[, policy: dict]]) -> str

        Return the content of a UDF module which is registered with the cluster.

        :param str module: the UDF module to read from the cluster.
        :param int language: :data:`aerospike.UDF_TYPE_LUA`
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

        .. seealso:: `Record UDF <https://docs.aerospike.com/server/guide/record_udf>`_ \
          and `Developing Record UDFs <https://developer.aerospike.com/udf/developing_record_udfs>`_.

    .. method:: scan_apply(ns, set, module, function[, args[, policy: dict[, options]]]) -> int

        .. deprecated:: 7.0.0 :class:`aerospike.Query` should be used instead.

        Initiate a scan and apply a record UDF to each record matched by the scan.

        This method blocks until the scan is complete.

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

    .. method:: query_apply(ns, set, predicate, module, function[, args[, policy: dict]]) -> int

        Initiate a query and apply a record UDF to each record matched by the query.

        This method blocks until the query is complete.

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

    .. method:: job_info(job_id, module[, policy: dict]) -> dict

        Return the status of a job running in the background.

        The returned :class:`dict` contains these keys:

            * ``"status"``: see :ref:`aerospike_job_constants_status` for possible values.
            * ``"records_read"``: number of scanned records.
            * ``"progress_pct"``: progress percentage of the job

        :param int job_id: the job ID returned by :meth:`scan_apply` or :meth:`query_apply`.
        :param module: one of :ref:`aerospike_job_constants`.
        :param policy: optional :ref:`aerospike_info_policies`.
        :returns: :class:`dict`
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

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

    .. method:: set_xdr_filter(data_center, namespace, expression_filter[, policy: dict]) -> str

        Set the cluster's xdr filter using an Aerospike expression.

        The cluster's current filter can be removed by setting expression_filter to None.

        :param str data_center: The data center to apply the filter to.
        :param str namespace: The namespace to apply the filter to.
        :param AerospikeExpression expression_filter: The filter to set. See expressions at :py:mod:`aerospike_helpers`.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: `xdr-set-filter Info Command Reference <https://docs.aerospike.com/reference/info#xdr-set-filter>`_.

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
        See `Truncate command reference <https://docs.aerospike.com/reference/info#truncate>`_.

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

    .. method:: index_string_create(ns, set, bin, name[, policy: dict])

        Create a string index with *index_name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: index_integer_create(ns, set, bin, name[, policy])

        Create an integer index with *name* on the *bin* in the specified \
        *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: index_list_create(ns, set, bin, index_datatype, name[, policy: dict])

        Create an index named *name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a list.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

    .. method:: index_map_keys_create(ns, set, bin, index_datatype, name[, policy: dict])

        Create an index named *name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the keys of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

    .. method:: index_map_values_create(ns, set, bin, index_datatype, name[, policy: dict])

        Create an index named *name* for numeric, string or GeoJSON values \
        (as defined by *index_datatype*) on records of the specified *ns*, *set* \
        whose *bin* is a map. The index will include the values of the map.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param index_datatype: Possible values are ``aerospike.INDEX_STRING``, ``aerospike.INDEX_NUMERIC`` and ``aerospike.INDEX_GEO2DSPHERE``.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. note:: Requires server version >= 3.8.0

        .. code-block:: python

            import aerospike

            client = aerospike.client({ 'hosts': [ ('127.0.0.1', 3000)]})

            # assume the bin fav_movies in the set test.demo bin should contain
            # a dict { (str) _title_ : (int) _times_viewed_ }
            # create a secondary index for string values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_keys_create('test', 'demo', 'fav_movies', aerospike.INDEX_STRING, 'demo_fav_movies_titles_idx')
            # create a secondary index for integer values of test.demo records whose 'fav_movies' bin is a map
            client.index_map_values_create('test', 'demo', 'fav_movies', aerospike.INDEX_NUMERIC, 'demo_fav_movies_views_idx')
            client.close()

    .. method:: index_geo2dsphere_create(ns, set, bin, name[, policy: dict])

        Create a geospatial 2D spherical index with *name* on the *bin* \
        in the specified *ns*, *set*.

        :param str ns: the namespace in the aerospike cluster.
        :param str set: the set name.
        :param str bin: the name of bin the secondary index is built on.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. seealso:: :class:`aerospike.GeoJSON`, :mod:`aerospike.predicates`

        .. note:: Requires server version >= 3.7.0

        .. code-block:: python

            import aerospike

            client = aerospike.client({ 'hosts': [ ('127.0.0.1', 3000)]})
            client.index_geo2dsphere_create('test', 'pads', 'loc', 'pads_loc_geo')
            client.close()


    .. method:: index_remove(ns: str, name: str[, policy: dict])

        Remove the index with *name* from the namespace.

        :param str ns: the namespace in the aerospike cluster.
        :param str name: the name of the index.
        :param dict policy: optional :ref:`aerospike_info_policies`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

    .. method:: get_cdtctx_base64(ctx: list) -> str

        Get the base64 representation of aerospike CDT ctx.

        See :ref:`aerospike_operation_helpers.cdt_ctx` for more details on CDT context.

        :param list ctx: Aerospike CDT context: generated by aerospike CDT ctx helper :mod:`aerospike_helpers`.
        :raises: a subclass of :exc:`~aerospike.exception.AerospikeError`.

        .. include:: examples/get_cdtctx_base64.py
            :code: python

        .. versionchanged:: 7.1.1


    .. index::
        single: Admin Operations

Admin Operations
----------------

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

.. seealso:: `Security features article <https://docs.aerospike.com/server/guide/security/index.html>`_.

.. class:: Client
    :noindex:

    .. method:: admin_create_role(role, privileges[, policy: dict[, whitelist[, read_quota[, write_quota]]]])

        Create a custom role containing a :class:`list` of privileges, as well as an optional whitelist and quotas.

        :param str role: The name of the role.
        :param list privileges: A list of :ref:`aerospike_privilege_dict`.
        :param dict policy: See :ref:`aerospike_admin_policies`.
        :param list whitelist: A list of whitelist IP addresses that can contain wildcards, for example ``10.1.2.0/24``.
        :param int read_quota: Maximum reads per second limit. Pass in ``0`` for no limit.
        :param int write_quota: Maximum write per second limit, Pass in ``0`` for no limit.

        :raises: One of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_whitelist(role, whitelist[, policy: dict])

        Add a whitelist to a role.

        :param str role: The name of the role.
        :param list whitelist: List of IP strings the role is allowed to connect to.
            Setting this to :py:obj:`None` will clear the whitelist for that role.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :raises: One of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_quotas(role[, read_quota[, write_quota[, policy: dict]]])

        Add quotas to a role.

        :param str role: the name of the role.
        :param int read_quota: Maximum reads per second limit. Pass in ``0`` for no limit.
        :param int write_quota: Maximum write per second limit. Pass in ``0`` for no limit.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_drop_role(role[, policy: dict])

        Drop a custom role.

        :param str role: the name of the role.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_grant_privileges(role, privileges[, policy: dict])

        Add privileges to a role.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_revoke_privileges(role, privileges[, policy: dict])

        Remove privileges from a role.

        :param str role: the name of the role.
        :param list privileges: a list of :ref:`aerospike_privilege_dict`.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_get_role(role[, policy: dict]) -> {}

        Get a :class:`dict` of privileges, whitelist, and quotas associated with a role.

        :param str role: the name of the role.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :return: a :ref:`aerospike_role_dict`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_get_roles([policy: dict]) -> {}

        Get the names of all roles and their attributes.

        :param dict policy: See :ref:`aerospike_admin_policies`.

        :return: a :class:`dict` of :ref:`aerospike_role_dict` keyed by role names.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_role(role[, policy: dict]) -> []

        Get the :class:`list` of privileges associated with a role.

        :param str role: the name of the role.
        :param dict policy: See :ref:`aerospike_admin_policies`.

        :return: a :class:`list` of :ref:`aerospike_privilege_dict`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_roles([policy: dict]) -> {}

        Get all named roles and their privileges.

        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :return: a :class:`dict` of :ref:`aerospike_privilege_dict` keyed by role name.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_create_user(username, password, roles[, policy: dict])

        Create a user and grant it roles.

        :param str username: the username to be added to the Aerospike cluster.
        :param str password: the password associated with the given username.
        :param list roles: the list of role names assigned to the user.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_drop_user(username[, policy: dict])

        Drop the user with a specified username from the cluster.

        :param str username: the username to be dropped from the aerospike cluster.

        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_change_password(username, password[, policy: dict])

        Change the password of a user.

        This operation can only be performed by that same user.

        :param str username: the username of the user.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_set_password(username, password[, policy: dict])

        Set the password of a user by a user administrator.

        :param str username: the username to be added to the aerospike cluster.
        :param str password: the password associated with the given username.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_grant_roles(username, roles[, policy: dict])

        Add roles to a user.

        :param str username: the username of the user.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_revoke_roles(username, roles[, policy: dict])

        Remove roles from a user.

        :param str username: the username to have the roles revoked.
        :param list roles: a list of role names.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

    .. method:: admin_query_user_info (user: str[, policy: dict]) -> dict

        Retrieve roles and other info for a given user.

        :param str user: the username of the user.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :return: a :class:`dict` of user data. See :ref:`admin_user_dict`.

    .. method:: admin_query_users_info ([policy: dict]) -> list

        Retrieve roles and other info for all users.

        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :return: a :class:`list` of users' data. See :ref:`admin_user_dict`.

    .. method:: admin_query_user (username[, policy: dict]) -> []

        Return the list of roles granted to the specified user.

        :param str username: the username of the user.
        :param dict policy: optional :ref:`aerospike_admin_policies`.

        :return: a :class:`list` of role names.

        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. deprecated:: 12.0.0 :meth:`admin_query_user_info` should be used instead.

    .. method:: admin_query_users ([policy: dict]) -> {}

        Get the roles of all users.

        :param dict policy: optional :ref:`aerospike_admin_policies`.
        :return: a :class:`dict` of roles keyed by username.
        :raises: one of the :exc:`~aerospike.exception.AdminError` subclasses.

        .. deprecated:: 12.0.0 :meth:`admin_query_users_info` should be used instead.

.. _admin_user_dict:

User Dictionary
===============

The user dictionary has the following key-value pairs:

    * ``"read_info"`` (:class:`list[int]`): list of read statistics.
      List may be :py:obj:`None`. Current statistics by offset are:

       * 0: read quota in records per second

       * 1: single record read transaction rate (TPS)

       * 2: read scan/query record per second rate (RPS)

       * 3: number of limitless read scans/queries

    Future server releases may add additional statistics.

    * ``"write_info"`` (:class:`list[int]`): list of write statistics.
      List may be :py:obj:`None`. Current statistics by offset are:

       * 0: write quota in records per second

       * 1: single record write transaction rate (TPS)

       * 2: write scan/query record per second rate (RPS)

       * 3: number of limitless write scans/queries

    Future server releases may add additional statistics.

    * ``"conns_in_use"`` (:class:`int`): number of currently open connections.

    * ``"roles"`` (:class:`list[str]`): list of assigned role names.

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

        See :ref:`aerospike.Query` for more details.

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
        client = aerospike.client(config)

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

    .. seealso:: `Data Model: Keys and Digests <https://docs.aerospike.com/server/architecture/data-model#records>`_.

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

    We reuse the code example in the key-tuple section and print the ``meta`` and ``bins`` values that were returned from :meth:`~aerospike.Client.get()`:

        .. code-block:: python

            import aerospike

            # NOTE: change this to your Aerospike server's seed node address
            seedNode = ('127.0.0.1', 3000)
            config = {'hosts': [seedNode]}
            client = aerospike.client(config)

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

.. _aerospike_policies:

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
            | Default: ``30000``
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
            | Default: ``30000``
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
            | Default: ``30000``
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
            | Default: ``30000``
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
            | Default: ``30000``
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
            | Default: ``30000``
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
        * **allow_inline_ssd** (:class:`bool`)
            Allow batch to be processed immediately in the server's receiving thread for SSD namespaces.
            If false, the batch will always be processed in separate service threads. Server versions < 6.0 ignore this field.

            Inline processing can introduce the possibility of unfairness because the server can process the entire
            batch before moving onto the next command.

            Default: ``False``

        * **deserialize** (:class:`bool`)
            | Should raw bytes be deserialized to as_list or as_map. Set to `False` for backup programs that just need access to raw bytes.
            |
            | Default: ``True``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.
        * **respond_all_keys** :class:`bool`
            Should all batch keys be attempted regardless of errors. This field is used on both the client and server.
            The client handles node specific errors and the server handles key specific errors.

            If ``True``, every batch key is attempted regardless of previous key specific errors.
            Node specific errors such as timeouts stop keys to that node, but keys directed at other nodes will continue
            to be processed.

            If ``False``, the server will stop the batch to its node on most key specific errors. The exceptions are
            ``AEROSPIKE_ERR_RECORD_NOT_FOUND`` and ``AEROSPIKE_FILTERED_OUT`` which never stop the batch. The client
            will stop the entire batch on node specific errors for sync commands that are run in sequence
            (``concurrent`` == false).
            The client will not stop the entire batch for async commands or sync commands run in parallel.

            Server versions < 6.0 do not support this field and treat this value as false for key specific errors.

            Default: ``True``

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
        * **ttl** :class:`int`
            | The time-to-live (expiration) in seconds to apply to every record in the batch.
            |
            | The ttl must be a 32-bit unsigned integer, or a :exc:`~aerospike.exception.ParamError` will be raised.
            |
            | Default: ``0``

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

.. object:: Role

    A :class:`dict` describing attributes associated with a specific role:

    .. hlist::
        :columns: 1

        * ``"privileges"``: a :class:`list` of :ref:`aerospike_privilege_dict`.
        * ``"whitelist"``: a :class:`list` of IP address strings.
        * ``"read_quota"``: a :class:`int` representing the allowed read transactions per second.
        * ``"write_quota"``: a :class:`int` representing the allowed write transactions per second.

.. _aerospike_privilege_dict:

Privilege Objects
-----------------

.. object:: privilege

    A :class:`dict` describing a privilege and where it applies to:

    .. hlist::
        :columns: 1

        * ``"code"``: one of the :ref:`aerospike_privileges` values
        * ``"ns"``: **optional** :class:`str` specifying the namespace where the privilege applies.

            If not specified, the privilege applies globally.
        * ``"set"``: **optional** :class:`str` specifying the set within the namespace where the privilege applies.

            If not specified, the privilege applies to the entire namespace.

    Example::

        {'code': aerospike.PRIV_READ, 'ns': 'test', 'set': 'demo'}


.. _aerospike_partition_objects:

Partition Objects
-----------------

.. object:: partition_filter

    A :class:`dict` of partition information used by the client to perform partition queries or scans.
    Useful for resuming terminated queries and querying particular partitions or records.

    .. hlist::
        :columns: 1

        * ``"begin"``: **Optional** :class:`int` signifying which partition to start at.

            Default: ``0`` (the first partition)
        * ``"count"``: **Optional** :class:`int` signifying how many partitions to process.

            Default: ``4096`` (all partitions)
        * ``"digest"``: **Optional** :class:`dict` containing the keys "init" and "value" signifying whether the digest has been calculated, and the digest value.

            * ``"init"``: :class:`bool` Whether the digest has been calculated.
            * ``"value"``: :class:`bytearray` The bytearray value of the digest, should be 20 characters long.

            Default: ``{}`` (will start from first record in partition)
        * ``"partition_status"``: **Optional** :class:`dict` containing partition_status tuples. These can be used to resume a query/scan.

            Default: ``{}`` (all partitions)

    Default: ``{}`` (All partitions will be queried/scanned).

    .. code-block:: python

        # Example of a query policy using partition_filter.

        # partition_status is most easily used to resume a query
        # and can be obtained by calling Query.get_partitions_status()
        partition_status = {
            0: {0, False, False, bytearray([0]*20)}...
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
    to set the partition status of a partition query or scan.

    This is useful for resuming either of those.

    The dictionary contains these key-value pairs:

    * ``"retry"``: :class:`bool` represents the overall retry status of this partition query. (i.e. Does this query/scan need to be retried?)

    * ``"done"``: :class:`bool` represents whether all partitions were finished.

    In addition, the dictionary contains keys of the partition IDs (:class:`int`),
    and each partition ID is mapped to a tuple containing the status details of a partition.

    That tuple has the following values in this order:

    .. hlist::
        :columns: 1

        * ``id``: :class:`int` represents a partition ID number
        * ``init``: :class:`bool` represents whether the digest being queried was calculated.
        * ``retry``: :class:`bool` represents whether this partition should be retried.
        * ``digest``: :class:`bytearray` represents the digest of the record being queried.

            Should be 20 characters long.
        * ``bval``: :class:`int` is used in conjunction with ``"digest"`` to determine the last record received by a partition query.

    Default: ``{}`` (All partitions will be queried).

    .. code-block:: python

       # Example of a query policy using partition_status.

       # Here is the form of partition_status.
       # partition_status = {
       #     0: (0, False, False, bytearray([0]*20), 0)...
       # }
       partition_status = query.get_partitions_status()

       policy = {
           "partition_filter": {
               "partition_status": partition_status,
               "begin": 0,
               "count": 4096
           },
       }
