.. _aerospike.Query:

.. currentmodule:: aerospike

========================================
:class:`aerospike.Query` --- Query Class
========================================

Overview
========

The query object created by calling :meth:`aerospike.query` is used for executing queries over a secondary index of a specified set. \
This set can be ommitted or be set to :py:obj:`None`. \
The :py:obj:`None` set contains those records which are not part of any named set.

The query can (optionally) be assigned either the \
:meth:`~aerospike.predicates.between` or :meth:`~aerospike.predicates.equals` predicate using :meth:`~aerospike.Query.where`. \
Otherwise, a query without a predicate will match all the records in the given set, \
similar to a :class:`~aerospike.Scan`.

The query is invoked using :meth:`~aerospike.Query.foreach`, :meth:`~aerospike.Query.results`, or :meth:`~aerospike.Query.execute_background`. \
The returned bins can be filtered by using :meth:`select`.

If a list of write operations is added to the query with :meth:`~aerospike.Query.add_ops`, \
they will be applied to each record processed by the query. \
See available write operations at :mod:`aerospike_helpers`.

Finally, a `stream UDF <http://www.aerospike.com/docs/udf/developing_stream_udfs.html>`_ \
may be applied with :meth:`~aerospike.Query.apply`. It will aggregate results out of the \
records streaming back from the query.

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

        .. code-block:: python

            import aerospike
            from aerospike import predicates as p
            import pprint

            config = { 'hosts': [ ('127.0.0.1', 3000)]}
            client = aerospike.client(config).connect()

            pp = pprint.PrettyPrinter(indent=2)
            query = client.query('test', 'demo')
            query.select('name', 'age') # matched records return with the values of these bins

            # assuming there is a secondary index on the 'age' bin of test.demo
            query.where(p.equals('age', 40))

            records = query.results( {'total_timeout':2000})

            pp.pprint(records)
            client.close()

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
        If the query is using the "partition_filter" query policy the callback will recieve two arguments
        The first is a :class:`int` representing partition id, the second is the same :ref:`aerospike_record_tuple`
        as a normal callback.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_query_policies`.
        :param dict options: optional :ref:`aerospike_query_options`.

        .. code-block:: python

            pp = pprint.PrettyPrinter(indent=2)
            query = client.query('test', 'demo')
            query.select('name', 'age') # matched records return with the values of these bins
            # assuming there is a secondary index on the 'age' bin of test.demo
            query.where(p.between('age', 20, 30))
            names = []
            def matched_names(record):
                key, metadata, bins = record
                pp.pprint(bins)
                names.append(bins['name'])

            query.foreach(matched_names, {'total_timeout':2000})
            pp.pprint(names)
            client.close()

        .. note:: To stop the stream return ``False`` from the callback function.

            .. code-block:: python

                import aerospike
                from aerospike import predicates as p

                config = { 'hosts': [ ('127.0.0.1',3000)]}
                client = aerospike.client(config).connect()

                def limit(lim, result):
                    c = [0] # integers are immutable so a list (mutable) is used for the counter
                    def key_add(record):
                        key, metadata, bins = record
                        if c[0] < lim:
                            result.append(key)
                            c[0] = c[0] + 1
                        else:
                            return False
                    return key_add

                query = client.query('test','user')
                query.where(p.between('age', 20, 30))
                keys = []
                query.foreach(limit(100, keys))
                print(len(keys)) # this will be 100 if the number of matching records > 100
                client.close()
        
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
        :param list arguments: optional arguments to pass to the *function*. NOTE: these arguments must be types supported by Aerospike See: `supported data types <http://www.aerospike.com/docs/guide/data-types.html>`_.
            If you need to use an unsuported type, (e.g. set or tuple) you can use a serializer like pickle first.
        :return: one of the supported types, :class:`int`, :class:`str`, :class:`float` (double), :class:`list`, :class:`dict` (map), :class:`bytearray` (bytes), :class:`bool`.

        .. seealso:: `Developing Stream UDFs <http://www.aerospike.com/docs/udf/developing_stream_udfs.html>`_

        .. note::

            Assume we registered the following Lua module with the cluster as \
            **stream_udf.lua** using :meth:`aerospike.udf_put`.

            .. code-block:: lua

                 local function having_ge_threshold(bin_having, ge_threshold)
                     return function(rec)
                         debug("group_count::thresh_filter: %s >  %s ?", tostring(rec[bin_having]), tostring(ge_threshold))
                         if rec[bin_having] < ge_threshold then
                             return false
                         end
                         return true
                     end
                 end

                 local function count(group_by_bin)
                   return function(group, rec)
                     if rec[group_by_bin] then
                       local bin_name = rec[group_by_bin]
                       group[bin_name] = (group[bin_name] or 0) + 1
                       debug("group_count::count: bin %s has value %s which has the count of %s", tostring(bin_name), tostring(group[bin_name]))
                     end
                     return group
                   end
                 end

                 local function add_values(val1, val2)
                   return val1 + val2
                 end

                 local function reduce_groups(a, b)
                   return map.merge(a, b, add_values)
                 end

                 function group_count(stream, group_by_bin, bin_having, ge_threshold)
                   if bin_having and ge_threshold then
                     local myfilter = having_ge_threshold(bin_having, ge_threshold)
                     return stream : filter(myfilter) : aggregate(map{}, count(group_by_bin)) : reduce(reduce_groups)
                   else
                     return stream : aggregate(map{}, count(group_by_bin)) : reduce(reduce_groups)
                   end
                 end

            Find the first name distribution of users in their twenties using \
            a query aggregation:

            .. code-block:: python

                import aerospike
                from aerospike import predicates as p
                import pprint

                config = {'hosts': [('127.0.0.1', 3000)],
                          'lua': {'system_path':'/usr/local/aerospike/lua/',
                                  'user_path':'/usr/local/aerospike/usr-lua/'}}
                client = aerospike.client(config).connect()

                pp = pprint.PrettyPrinter(indent=2)
                query = client.query('test', 'users')
                query.where(p.between('age', 20, 29))
                query.apply('stream_udf', 'group_count', [ 'first_name' ])
                names = query.results()
                # we expect a dict (map) whose keys are names, each with a count value
                pp.pprint(names)
                client.close()

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

        .. code-block:: python

            import aerospike
            from aerospike_helpers.operations import list_operations
            from aerospike_helpers.operations import operations
            query = client.query('test', 'demo')

            ops =  [
                operations.append(test_bin, 'val_to_append'),
                list_operations.list_remove_by_index(test_bin, list_index_to_remove, aerospike.LIST_RETURN_NONE)
            ]
            query.add_ops(ops)

            id = query.execute_background()
            client.close()

        For a more comprehensive example, see using a list of write ops with :meth:`Query.execute_background` .

    .. method:: execute_background([, policy])

        Execute a record UDF or write operations on records found by the query in the background. This method returns before the query has completed.
        A UDF or a list of write operations must have been added to the query with :meth:`Query.apply` or :meth:`Query.add_ops` respectively.

        :param dict policy: optional :ref:`aerospike_write_policies`.

        :return: a job ID that can be used with :meth:`aerospike.job_info` to track the status of the ``aerospike.JOB_QUERY`` , as it runs in the background.

        .. code-block:: python

            # Using a record UDF
            import aerospike
            query = client.query('test', 'demo')
            query.apply('myudfs', 'myfunction', ['a', 1])
            query_id = query.execute_background()
            # This id can be used to monitor the progress of the background query

        .. code-block:: python

            # Using a list of write ops.
            import aerospike
            from aerospike import predicates
            from aerospike import exception as ex
            from aerospike_helpers.operations import list_operations
            import sys
            import time

            # Configure the client.
            config = {"hosts": [("127.0.0.1", 3000)]}

            # Create a client and connect it to the cluster.
            try:
                client = aerospike.client(config).connect()
            except ex.ClientError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)

            TEST_NS = "test"
            TEST_SET = "demo"
            nested_list = [{"name": "John", "id": 100}, {"name": "Bill", "id": 200}]
            # Write the records.
            try:
                keys = [(TEST_NS, TEST_SET, i) for i in range(5)]
                for i, key in enumerate(keys):
                    client.put(key, {"account_number": i, "members": nested_list})
            except ex.RecordError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))

            # EXAMPLE 1: Append a new account member to all accounts.
            try:
                new_member = {"name": "Cindy", "id": 300}

                ops = [list_operations.list_append("members", new_member)]

                query = client.query(TEST_NS, TEST_SET)
                query.add_ops(ops)
                id = query.execute_background()
                # allow for query to complete
                time.sleep(3)
                print("EXAMPLE 1")

                for i, key in enumerate(keys):
                    _, _, bins = client.get(key)
                    print(bins)
            except ex.ClientError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)

            # EXAMPLE 2: Remove a member from a specific account using predicates.
            try:
                # Add index to the records for use with predex.
                client.index_integer_create(
                    TEST_NS, TEST_SET, "account_number", "test_demo_account_number_idx"
                )

                ops = [
                    list_operations.list_remove_by_index("members", 0, aerospike.LIST_RETURN_NONE)
                ]

                query = client.query(TEST_NS, TEST_SET)
                number_predicate = predicates.equals("account_number", 3)
                query.where(number_predicate)
                query.add_ops(ops)
                id = query.execute_background()
                # allow for query to complete
                time.sleep(3)
                print("EXAMPLE 2")

                for i, key in enumerate(keys):
                    _, _, bins = client.get(key)
                    print(bins)
            except ex.ClientError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)

            # Cleanup and close the connection to the Aerospike cluster.
            for i, key in enumerate(keys):
                client.remove(key)
            client.index_remove(TEST_NS, "test_demo_account_number_idx")
            client.close()

            """
            EXPECTED OUTPUT:
            EXAMPLE 1
            {'account_number': 0, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 1, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 2, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 3, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 4, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            EXAMPLE 2
            {'account_number': 0, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 1, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 2, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 3, 'members': [{'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            {'account_number': 4, 'members': [{'name': 'John', 'id': 100}, {'name': 'Bill', 'id': 200}, {'name': 'Cindy', 'id': 300}]}
            """

    .. method:: paginate()

        Makes a query instance a paginated query.
        Call this if you are using the max_records and you need to query data in pages.

        .. note::
            Calling .paginate() on a query instance causes it to save its partition state.
            This can be retrieved later using .get_partitions_status(). This can also been done by
            using the partition_filter policy.

        .. code-block:: python

            # Query 3 pages of 1000 records each.

            import aerospike

            pages = 3
            page_size = 1000

            query = client.query('test', 'demo')
            query.max_records = 1000

            query.paginate()

            # NOTE: The number of pages queried and records returned per page can differ
            # if record counts are small or unbalanced across nodes.
            for page in range(pages):
                records = query.results()

                print("got page: " + str(page))

                if query.is_done():
                    print("all done")
                    break

            # This id can be used to paginate queries.

    .. method:: is_done()

        If using query pagination, did the previous paginated or partition_filter query using this query instance return all records?

        :return: A :class:`bool` signifying whether this paginated query instance has returned all records.

        .. code-block:: python

            import aerospike

            query = client.query('test', 'demo')
            query.max_records = 1000

            query.paginate()

            records = query.results(policy=policy)

            if query.is_done():
                print("all done")

            # This id can be used to monitor the progress of a paginated query.

    .. method:: get_partitions_status()

        Get this query instance's partition status. That is which partitions have been queried and which have not.
        The returned value is a :class:`dict` with partition id, :class:`int`, as keys and :class:`tuple` as values.
        If the query instance is not tracking its partitions, the returned :class:`dict` will be empty.

        .. note::
            A query instance must have had .paginate() called on it, or been used with a partition filter, in order retrieve its
            partition status. If .paginate() was not called, or partition_filter was not used, the query instance will not save partition status.

        :return: a :class:`tuple` of form (id: :class:`int`, init: class`bool`, done: class`bool`, digest: :class:`bytearray`).
            See :ref:`aerospike_partition_objects` for more information.

        .. code-block:: python

            # This is an example of resuming a query using partition status.
            import aerospike


            for i in range(15):
                key = ("test", "demo", i)
                bins = {"id": i}
                client.put(key, bins)

            records = []
            resumed_records = []

            def callback(input_tuple):
                record, _, _ = input_tuple

                if len(records) == 5:
                    return False

                records.append(record)

            query = client.query("test", "demo")
            query.paginate()

            query.foreach(callback)

            # The first query should stop after 5 records.
            assert len(records) == 5

            partition_status = query.get_partitions_status()

            def resume_callback(part_id, input_tuple):
                record, _, _ = input_tuple
                resumed_records.append(record)

            query_resume = client.query("test", "demo")

            policy = {
                "partition_filter": {
                    "partition_status": partition_status
                },
            }

            query_resume.foreach(resume_callback, policy)

            # should be 15
            total_records = len(records) + len(resumed_records)
            print(total_records)

            # cleanup
            for i in range(15):
                key = ("test", "demo", i)
                client.remove(key)

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
            | If max_retries is exceeded, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``.
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
            | If ``total_timeout`` is not ``0`` ``total_timeout`` is reached before the transaction completes, the transaction will return error \
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
        * **fail_on_cluster_change** :class:`bool`
            | Deprecated in 6.0.0. No longer has any effect..
            | Terminate query if cluster is in migration state. 
            |
            | Default ``False``
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
