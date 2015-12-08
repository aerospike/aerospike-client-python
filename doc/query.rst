.. _query:

.. currentmodule:: aerospike

=================================
Query Class --- :class:`Query`
=================================

:class:`Query`
===============

.. class:: Query

    The Query object created by calling :meth:`aerospike.Client.query` is used \
    for executing queries over a secondary index of a specified set (which \
    can be omitted or :py:obj:`None`). For queries, the :py:obj:`None` set contains those \
    records which are not part of any named set.

    The Query can (optionally) be assigned one of the \
    :mod:`~aerospike.predicates` (:meth:`~aerospike.predicates.between` \
    or :meth:`~aerospike.predicates.equals`) using :meth:`where`. A query \
    without a predicate will match all the records in the given set, similar \
    to a :class:`~aerospike.Scan`.

    The query is invoked using either :meth:`foreach` or :meth:`results`. \
    The bins returned can be filtered by using :meth:`select`.

    Finally, a `stream UDF <http://www.aerospike.com/docs/udf/developing_stream_udfs.html>`_ \
    may be applied with :meth:`apply`. It will aggregate results out of the \
    records streaming back from the query.

    .. seealso::
        `Queries <http://www.aerospike.com/docs/guide/query.html>`_ and \
        `Managing Queries <http://www.aerospike.com/docs/operations/manage/queries/>`_.


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


    .. method:: results([policy]) -> list of (key, meta, bins)

        Buffer the records resulting from the query, and return them as a \
        :class:`list` of records.

        :param dict policy: optional :ref:`aerospike_query_policies`.
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
            records = query.results( {'timeout':2000})
            pp.pprint(records)
            client.close()

        .. note::

            Queries require a secondary index to exist on the *bin* being queried.


    .. method:: foreach(callback[, policy])

        Invoke the *callback* function for each of the records streaming back \
        from the query.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_query_policies`.

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
            def matched_names((key, metadata, bins)):
                pp.pprint(bins)
                names.append(bins['name'])

            query.foreach(matched_names, {'timeout':2000})
            pp.pprint(names)
            client.close()

        .. note:: To stop the stream return ``False`` from the callback function.

            .. code-block:: python

                from __future__ import print_function
                import aerospike
                from aerospike import predicates as p

                config = { 'hosts': [ ('127.0.0.1',3000)]}
                client = aerospike.client(config).connect()

                def limit(lim, result):
                    c = [0] # integers are immutable so a list (mutable) is used for the counter
                    def key_add((key, metadata, bins)):
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
        :param list arguments: optional arguments to pass to the *function*.
        :return: one of the supported types, :class:`int`, :class:`str`, :class:`float` (double), :class:`list`, :class:`dict` (map), :class:`bytearray` (bytes).

        .. seealso:: `Developing Stream UDFs <http://www.aerospike.com/docs/udf/developing_stream_udfs.html>`_

        .. note::

            Assume we registered the following Lua module with the cluster as \
            **stream_udf.lua** using :meth:`aerospike.Client.udf_put`.

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


.. _aerospike_query_policies:

Query Policies
--------------

.. object:: policy

    A :class:`dict` of optional query policies which are applicable to :meth:`Query.results` and :meth:`Query.foreach`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** maximum time in milliseconds to wait for the operation to complete. Default ``0`` means *do not timeout*.


