.. _aerospike.query:

.. currentmodule:: aerospike

=================================
Query Class --- :class:`Query`
=================================

:class:`Query`
===============

    The query object created by calling :meth:`aerospike.query` is used \
    for executing queries over a secondary index of a specified set (which \
    can be omitted or :py:obj:`None`). For queries, the :py:obj:`None` set contains those \
    records which are not part of any named set.

    The query can (optionally) be assigned one of the following \

    * One of the :mod:`~aerospike.predicates` (:meth:`~aerospike.predicates.between` or :meth:`~aerospike.predicates.equals`) using :meth:`~aerospike.Query.where`. \
    * A list of :mod:`~aerospike.predexp` using :meth:`~aerospike.Query.predexp` \
    
    A query without a predicate will match all the records in the given set, similar \
    to a :class:`~aerospike.Scan`.

    The query is invoked using :meth:`~aerospike.Query.foreach`, :meth:`~aerospike.Query.results`, or :meth:`~aerospike.Query.execute_background` \
    The bins returned can be filtered by using :meth:`select`.

    If a list of write operations is added to the query with :meth:`~aerospike.Query.add_ops`, they will be applied to each record processed by the query. See available write operations at See :mod:`aerospike_helpers` \

    Finally, a `stream UDF <http://www.aerospike.com/docs/udf/developing_stream_udfs.html>`_ \
    may be applied with :meth:`~aerospike.Query.apply`. It will aggregate results out of the \
    records streaming back from the query.

    .. seealso::
        `Queries <http://www.aerospike.com/docs/guide/query.html>`_ and \
        `Managing Queries <http://www.aerospike.com/docs/operations/manage/queries/>`_.


Query Methods
-------------
.. class:: Query

    .. method:: select(bin1[, bin2[, bin3..]])

        Set a filter on the record bins resulting from :meth:`results` or \
        :meth:`foreach`. If a selected bin does not exist in a record it will \
        not appear in the *bins* portion of that record tuple.


    .. method:: where(predicate)

        Set a where *predicate* for the query, without which the query will \
        behave similar to :class:`aerospike.Scan`. The predicate is produced by \
        one of the :mod:`aerospike.predicates` methods :meth:`~aerospike.predicates.equals` \
        and :meth:`~aerospike.predicates.between`.

        :param tuple predicate: the :py:func:`tuple` produced by one of the :mod:`aerospike.predicates` methods.

        .. note:: Currently, you can assign at most one predicate to the query.


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

        .. note::

            Queries require a secondary index to exist on the *bin* being queried.
        
    .. note::
        Python client version >= 3.10.0 Supports predicate expressions for results, foreach, and execute_background see :mod:`~aerospike.predexp`.
        Requires server versions >= 4.7.0.

        .. code-block:: python

            import aerospike
            from aerospike import predexp
            from aerospike import exception as ex
            import sys
            import time

            config = {"hosts": [("127.0.0.1", 3000)]}
            client = aerospike.client(config).connect()

            # register udf
            try:
                client.udf_put(
                    "/path/to/my_udf.lua"
                )
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                client.close()
                sys.exit(1)

            # put records and apply udf
            try:
                keys = [("test", "demo", 1), ("test", "demo", 2), ("test", "demo", 3)]
                records = [{"number": 1}, {"number": 2}, {"number": 3}]
                for i in range(3):
                    client.put(keys[i], records[i])

                try:
                    client.index_integer_create("test", "demo", "number", "test_demo_number_idx")
                except ex.IndexFoundError:
                    pass

                query = client.query("test", "demo")
                query.apply("my_udf", "my_udf", ["number", 10])
                job_id = query.execute_background()

                # wait for job to finish
                while True:
                    response = client.job_info(job_id, aerospike.JOB_SCAN)
                    print(response)
                    if response["status"] != aerospike.JOB_STATUS_INPROGRESS:
                        break
                    time.sleep(0.25)

                records = client.get_many(keys)
                print(records)
            except ex.AerospikeError as e:
                print("Error: {0} [{1}]".format(e.msg, e.code))
                sys.exit(1)
            finally:
                client.close()
            # EXPECTED OUTPUT:
            # [
            #   (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 2, 'ttl': 2591999}, {'number': 11}),
            #   (('test', 'demo', 2, bytearray(b'\xaejQ_7\xdeJ\xda\xccD\x96\xe2\xda\x1f\xea\x84\x8c:\x92p')), {'gen': 12, 'ttl': 2591999}, {'number': 12}),
            #   (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 13, 'ttl': 2591999}, {'number': 13})
            # ]
        
        .. code-block:: python

            # contents of my_udf.lua
            function my_udf(rec, bin, offset)
                info("my transform: %s", tostring(record.digest(rec)))
                rec[bin] = rec[bin] + offset
                aerospike:update(rec)
            end
        
        .. note::
            For a similar example using .results() see :meth:`aerospike.Scan.results`.


    .. method:: foreach(callback[, policy [, options]])

        Invoke the *callback* function for each of the records streaming back \
        from the query.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_query_policies`.
        :param dict options: optional :ref:`aerospike_query_options`.

        .. note:: A :ref:`aerospike_record_tuple` is passed as the argument to the callback function.

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

    .. method:: predexp(predicates)

        Set the predicate expression filters to be used by this query.

        :param predicates: `list` A list of predicates generated by the :ref:`aerospike.predexp` functions

        .. code-block:: python

            import aerospike
            from aerospike import predexp as predexp
            query = client.query('test', 'demo')

            predexps =  [
                predexp.rec_device_size(),
                predexp.integer_value(65 * 1024),
                predexp.integer_greater()
            ]
            query.predexp(predexps)

            big_records = query.results()
            client.close()

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

.. _aerospike_query_policies:

Query Policies
--------------

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
        * **expressions** :class:`list`
            | Compiled aerospike expressions :mod:`aerospike_helpers` used for filtering records within a transaction.
            |
            | Default: None

            .. note:: Requires Aerospike server version >= 5.2.

.. _aerospike_query_options:

Query Options
--------------

.. object:: options

    A :class:`dict` of optional query options which are applicable to :meth:`Query.foreach` and :meth:`Query.results`.

    .. hlist::
        :columns: 1

        * **nobins** :class:`bool` 
            | Whether to return the *bins* portion of the :ref:`aerospike_record_tuple`. 
            | 
            | Default ``False``.

    .. versionadded:: 3.0.0
