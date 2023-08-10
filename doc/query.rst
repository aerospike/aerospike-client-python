.. _aerospike.Query:

.. currentmodule:: aerospike

========================================
:class:`aerospike.Query` --- Query Class
========================================

Overview
========

Constructing A Query
--------------------

The query object is used for executing queries over a secondary index of a specified set.
It can be created by calling :meth:`aerospike.Client.query`.

A query without a secondary index filter will apply to all records in the namespace,
similar to a :class:`~aerospike.Scan`.

Otherwise, the query can optionally be assigned one of the secondary index filters in :mod:`aerospike.predicates`
to filter out records using their bin values.
These secondary index filters are applied to the query using :meth:`~aerospike.Query.where`.
In this case, if the set is initialized to :py:obj:`None`, then the query will only apply to records without a set.

.. note::
    The secondary index filters in :mod:`aerospike.predicates` are **not** the same as
    the deprecated `predicate expressions <https://docs.aerospike.com/server/guide/predicate>`_.
    For more details, read this `guide <https://docs.aerospike.com/server/guide/query>`_.

Writing Using Query
-------------------

If a list of write operations is added to the query with :meth:`~aerospike.Query.add_ops`, \
they will be applied to each record processed by the query. \
See available write operations at :mod:`aerospike_helpers.operations`.

Query Aggregations
------------------

A `stream UDF <https://developer.aerospike.com/udf/developing_stream_udfs>`_ \
may be applied with :meth:`~aerospike.Query.apply`. It will aggregate results out of the \
records streaming back from the query.

Getting Results From Query
--------------------------

The returned bins can be filtered by using :meth:`select`.

Finally, the query is invoked using one of these methods:

- :meth:`~aerospike.Query.foreach`
- :meth:`~aerospike.Query.results`
- :meth:`~aerospike.Query.execute_background`

.. seealso::
    `Queries <http://www.aerospike.com/docs/guide/query.html>`_ and \
    `Managing Queries <http://www.aerospike.com/docs/operations/manage/queries/>`_.

Fields
======

.. class:: Query

    max_records (:class:`int`)
        Approximate number of records to return to client.

        This number is divided by the number of nodes involved in the scan.
        The actual number of records returned may be less than ``max_records`` if node record counts are small and unbalanced across nodes.

        Default: ``0`` (no limit)

        .. note::
            Requires server version >= 6.0.0

    records_per_second (:class:`int`)
        Limit the scan to process records at records_per_second.
        Requires server version >= 6.0.0

        Default: ``0`` (no limit)

    ttl (:class:`int`)
        The time-to-live (expiration) of the record in seconds.

        There are also special values that can be set in the record TTL:

            ``0`` (``TTL_NAMESPACE_DEFAULT``)
                Which means that the record will adopt the default TTL value from the namespace.

            ``0xFFFFFFFF`` (``TTL_NEVER_EXPIRE``)
                (also, ``-1`` in a signed 32 bit int) Which means that the record will never expire.

            ``0xFFFFFFFE`` (``TTL_DONT_UPDATE``)
                (also, ``-2`` in a signed 32 bit int)
                Which means that the record ttl will not change when the record is
                updated.

        .. note::
            Note that the TTL value will be employed ONLY on background query writes.

        Requires server version >= 6.0.0

        Default: ``0`` (record will adopt the default TTL value from the namespace)

Methods
=======

Assume this boilerplate code is run before all examples below:

.. include:: examples/query/boilerplate.py
    :code: python

.. class:: Query
    :noindex:

    .. method:: select(bin1[, bin2[, bin3..]])

        Set a filter on the record bins resulting from :meth:`results` or \
        :meth:`foreach`.

        If a selected bin does not exist in a record it will not appear in the *bins* portion of that record tuple.

    .. method:: where(predicate[, ctx])

        Set a where *predicate* for the query.

        You can only assign at most one predicate to the query.
        If this function isn't called, the query will behave similar to :class:`aerospike.Scan`.

        :param tuple predicate: the :class:`tuple` produced by either :meth:`~aerospike.predicates.equals` or :meth:`~aerospike.predicates.between`.
        :param list ctx: the :class:`list` produced by one of the :mod:`aerospike_helpers.cdt_ctx` methods.

    .. method:: results([,policy [, options]]) -> list of (key, meta, bins)

        Buffer the records resulting from the query, and return them as a \
        :class:`list` of records.

        :param dict policy: optional :ref:`aerospike_query_policies`.
        :param dict options: optional :ref:`aerospike_query_options`.
        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. include:: examples/query/results.py
            :code: python

        .. note:: As of client 7.0.0 and with server >= 6.0 results and the query policy
            "partition_filter" see :ref:`aerospike_partition_objects` can be used to specify which partitions/records
            results will query. See the example below.

            .. code-block:: python

                # This is an example of querying partitions 1000 - 1003.
                import aerospike


                query = client.query("test", "demo")

                policy = {
                    "partition_filter": {
                        "begin": 1000,
                        "count": 4
                    },
                }

            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # results will be the records in partitions 1000 - 1003
            results = query.results(policy=policy)


    .. method:: foreach(callback[, policy [, options]])

        Invoke the *callback* function for each of the records streaming back from the query.

        A :ref:`aerospike_record_tuple` is passed as the argument to the callback function.
        If the query is using the "partition_filter" query policy the callback will receive two arguments
        The first is a :class:`int` representing partition id, the second is the same :ref:`aerospike_record_tuple`
        as a normal callback.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_query_policies`.
        :param dict options: optional :ref:`aerospike_query_options`.

        .. include:: examples/query/foreach.py
            :code: python

        .. note:: To stop the stream return ``False`` from the callback function.

            .. include:: examples/query/foreachfalse.py
                :code: python

        .. note:: As of client 7.0.0 and with server >= 6.0 foreach and the query policy
         "partition_filter" see :ref:`aerospike_partition_objects` can be used to specify which partitions/records
         foreach will query. See the example below.

         .. code-block:: python

            # This is an example of querying partitions 1000 - 1003.
            import aerospike


            partitions = []

            def callback(part_id, input_tuple):
                print(part_id)
                partitions.append(part_id)

            query = client.query("test", "demo")

            policy = {
                "partition_filter": {
                    "begin": 1000,
                    "count": 4
                },
            }

            query.foreach(callback, policy)


            # NOTE that these will only be non 0 if there are records in partitions 1000 - 1003
            # should be 4
            print(len(partitions))

            # should be [1000, 1001, 1002, 1003]
            print(partitions)

    .. method:: apply(module, function[, arguments])

        Aggregate the :meth:`results` using a stream \
        `UDF <http://www.aerospike.com/docs/guide/udf.html>`_. If no \
        predicate is attached to the  :class:`~aerospike.Query` the stream UDF \
        will aggregate over all the records in the specified set.

        :param str module: the name of the Lua module.
        :param str function: the name of the Lua function within the *module*.
        :param list arguments: optional arguments to pass to the *function*. NOTE: these arguments must be types supported by Aerospike See: `supported data types <https://docs.aerospike.com/server/guide/data-types/overview>`_.
            If you need to use an unsupported type, (e.g. set or tuple) you must use your own serializer.

        .. seealso:: `Developing Stream UDFs <https://developer.aerospike.com/udf/developing_stream_udfs>`_

        Example: find the first name distribution of users who are 21 or older using \
        a query aggregation:

        .. include:: examples/lua/example.lua
            :code: Lua

        Assume the example code above is in a file called "example.lua", and is the same folder as the following script.

        .. include:: examples/lua/lua.py
            :code: python

        With stream UDFs, the final reduce steps (which ties
        the results from the reducers of the cluster nodes) executes on the
        client-side. Explicitly setting the Lua ``user_path`` in the
        config helps the client find the local copy of the module
        containing the stream UDF. The ``system_path`` is constructed when
        the Python package is installed, and contains system modules such
        as ``aerospike.lua``, ``as.lua``, and ``stream_ops.lua``.
        The default value for the Lua ``system_path`` is
        ``/usr/local/aerospike/lua``.

    .. method:: add_ops(ops)

        Add a list of write ops to the query.
        When used with :meth:`Query.execute_background` the query will perform the write ops on any records found.
        If no predicate is attached to the Query it will apply ops to all the records in the specified set.

        :param ops: `list` A list of write operations generated by the aerospike_helpers e.g. list_operations, map_operations, etc.

        .. note::
            Requires server version >= 4.7.0.

    .. method:: execute_background([, policy])

        Execute a record UDF or write operations on records found by the query in the background. This method returns before the query has completed.
        A UDF or a list of write operations must have been added to the query with :meth:`Query.apply` or :meth:`Query.add_ops` respectively.

        :param dict policy: optional :ref:`aerospike_write_policies`.

        :return: a job ID that can be used with :meth:`~aerospike.Client.job_info` to track the status of the ``aerospike.JOB_QUERY`` , as it runs in the background.

        .. code-block:: python

            # EXAMPLE 1: Increase everyone's score by 100

            from aerospike_helpers.operations import operations
            ops = [
                operations.increment("score", 100)
            ]
            query.add_ops(ops)
            id = query.execute_background()

            # Allow time for query to complete
            import time
            time.sleep(3)

            for key in keyTuples:
                _, _, bins = client.get(key)
                print(bins)
            # {"score": 200, "elo": 1400}
            # {"score": 120, "elo": 1500}
            # {"score": 110, "elo": 1100}
            # {"score": 300, "elo": 900}

            # EXAMPLE 2: Increase score by 100 again for those with elos > 1000
            # Use write policy to select players by elo
            import aerospike_helpers.expressions as expr
            eloGreaterOrEqualTo1000 = expr.GE(expr.IntBin("elo"), 1000).compile()
            writePolicy = {
                "expressions": eloGreaterOrEqualTo1000
            }
            id = query.execute_background(policy=writePolicy)

            time.sleep(3)

            for i, key in enumerate(keyTuples):
                _, _, bins = client.get(key)
                print(bins)
            # {"score": 300, "elo": 1400} <--
            # {"score": 220, "elo": 1500} <--
            # {"score": 210, "elo": 1100} <--
            # {"score": 300, "elo": 900}

            # Cleanup and close the connection to the Aerospike cluster.
            for key in keyTuples:
                client.remove(key)
            client.close()

    .. method:: paginate()

        Makes a query instance a paginated query.
        Call this if you are using the max_records and you need to query data in pages.

        .. note::
            Calling .paginate() on a query instance causes it to save its partition state.
            This can be retrieved later using .get_partitions_status(). This can also been done by
            using the partition_filter policy.

        .. code-block:: python

            # After inserting 4 records...
            # Query 3 pages of 2 records each.

            pages = 3
            page_size = 2

            query.max_records = 2
            query.paginate()

            # NOTE: The number of pages queried and records returned per page can differ
            # if record counts are small or unbalanced across nodes.
            for page in range(pages):
                records = query.results()
                print("got page: " + str(page))

                # Print records in each page
                for record in records:
                    print(record)

                if query.is_done():
                    print("all done")
                    break
            # got page: 0
            # (('test', 'demo', None, bytearray(b'HD\xd1\xfa$L\xa0\xf5\xa2~\xd6\x1dv\x91\x9f\xd6\xfa\xad\x18\x00')), {'ttl': 2591996, 'gen': 1}, {'score': 20, 'elo': 1500})
            # (('test', 'demo', None, bytearray(b'f\xa4\t"\xa9uc\xf5\xce\x97\xf0\x16\x9eI\xab\x89Q\xb8\xef\x0b')), {'ttl': 2591996, 'gen': 1}, {'score': 10, 'elo': 1100})
            # got page: 1
            # (('test', 'demo', None, bytearray(b'\xb6\x9f\xf5\x7f\xfarb.IeaVc\x17n\xf4\x9b\xad\xa7T')), {'ttl': 2591996, 'gen': 1}, {'score': 200, 'elo': 900})
            # (('test', 'demo', None, bytearray(b'j>@\xfe\xe0\x94\xd5?\n\xd7\xc3\xf2\xd7\x045\xbc*\x07 \x1a')), {'ttl': 2591996, 'gen': 1}, {'score': 100, 'elo': 1400})
            # got page: 2
            # all done

    .. method:: is_done()

        If using query pagination, did the previous paginated or partition_filter query using this query instance return all records?

        :return: A :class:`bool` signifying whether this paginated query instance has returned all records.

    .. method:: get_partitions_status()

        Get this query instance's partition status. That is which partitions have been queried and which have not.
        If the query instance is not tracking its partitions, the returned :class:`dict` will be empty.

        .. note::
            A query instance must have had .paginate() called on it, or been used with a partition filter, in order retrieve its
            partition status. If .paginate() was not called, or partition_filter was not used, the query instance will not save partition status.

        :return: See :ref:`aerospike_partition_objects` for a description of the partition status return value.

        .. code-block:: python

            # Only read 2 records

            recordCount = 0
            def callback(record):
                global recordCount
                if recordCount == 2:
                    return False
                recordCount += 1

                print(record)

            # Query is set to read ALL records
            query = client.query("test", "demo")
            query.paginate()
            query.foreach(callback)
            # (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 10, 'elo': 1100})
            # (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 20, 'elo': 1500})


            # Use this to resume query where we left off
            partition_status = query.get_partitions_status()

            # Callback must include partition_id parameter
            # if partition_filter is included in policy
            def resume_callback(partition_id, record):
                print(partition_id, "->", record)

            policy = {
                "partition_filter": {
                    "partition_status": partition_status
                },
            }

            query.foreach(resume_callback, policy)
            # 1096 -> (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 100, 'elo': 1400})
            # 3690 -> (('test', 'demo', None, bytearray(b'...')), {'ttl': 2591996, 'gen': 1}, {'score': 200, 'elo': 900})

.. _aerospike_query_policies:

Policies
========

.. object:: policy

    A :class:`dict` of optional query policies which are applicable to :meth:`Query.results` and :meth:`Query.foreach`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **max_retries** :class:`int`
            | Maximum number of retries before aborting the current transaction. The initial attempt is not counted as a retry.
            |
            | If max_retries is exceeded, the transaction will the last suberror that was received.
            |
            | Default: ``0``

            .. warning:: : Database writes that are not idempotent (such as "add") should not be retried because the write operation may be performed \
              multiple times if the client timed out previous transaction attempts. It's important to use a distinct write policy for non-idempotent writes \
              which sets max_retries = `0`;

        * **sleep_between_retries** :class:`int`
            | Milliseconds to sleep between retries. Enter ``0`` to skip sleep.
            |
            | Default: ``0``
        * **socket_timeout** :class:`int`
            | Socket idle timeout in milliseconds when processing a database command.
            |
            | If socket_timeout is not ``0`` and the socket has been idle for at least socket_timeout, both max_retries and total_timeout are checked. \
              If max_retries and total_timeout are not exceeded, the transaction is retried.
            |
            | If both ``socket_timeout`` and ``total_timeout`` are non-zero and ``socket_timeout`` > ``total_timeout``, then ``socket_timeout`` will be set to \
             ``total_timeout``. If ``socket_timeout`` is ``0``, there will be no socket idle limit.
            |
            | Default: ``30000``.
        * **total_timeout** :class:`int`
            | Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. \
             The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not ``0`` and ``total_timeout`` is reached before the transaction completes, the transaction will return error \
             ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is ``0``, there will be no total time limit.
            |
            | Default: ``0``
        * **compress** (:class:`bool`)
            | Compress client requests and server responses.
            |
            | Use zlib compression on write or batch read commands when the command buffer size is greater than 128 bytes. In addition, tell the server to compress it's response on read commands. The server response compression threshold is also 128 bytes.
            |
            | This option will increase cpu and memory usage (for extra compressed buffers), but decrease the size of data sent over the network.
            |
            | Default: ``False``
        * **deserialize** :class:`bool`
            | Should raw bytes representing a list or map be deserialized to a list or dictionary.
            | Set to `False` for backup programs that just need access to raw bytes.
            |
            | Default: ``True``
        * **short_query** :class:`bool`
            | Is query expected to return less than 100 records.
            | If True, the server will optimize the query for a small record set.
            | This field is ignored for aggregation queries, background queries
            | and server versions less than 6.0.0.
            |
            | Mututally exclusive with records_per_second
            | Default: ``False``
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

        * **partition_filter** :class:`dict`
            | A dictionary of partition information used by the client
            | to perform partiton queries. Useful for resuming terminated queries and
            | querying particular partitons/records.
            |
            |   See :ref:`aerospike_partition_objects` for more information.
            |
            | Default: ``{}`` (All partitions will be queried).

            .. note:: Requires Aerospike server version >= 6.0

        * **replica**
            | One of the :ref:`POLICY_REPLICA` values such as :data:`aerospike.POLICY_REPLICA_MASTER`
            |
            | Default: ``aerospike.POLICY_REPLICA_SEQUENCE``

.. _aerospike_query_options:

Options
=======

.. object:: options

    A :class:`dict` of optional query options which are applicable to :meth:`Query.foreach` and :meth:`Query.results`.

    .. hlist::
        :columns: 1

        * **nobins** :class:`bool`
            | Whether to return the *bins* portion of the :ref:`aerospike_record_tuple`.
            |
            | Default ``False``.

    .. versionadded:: 3.0.0
