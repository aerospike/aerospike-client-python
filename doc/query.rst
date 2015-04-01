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
    can be ommitted or ``None``).

    The Query can (optionally) be assigned one of the \
    :mod:`~aerospike.predicates` (:meth:`~aerospike.predicates.between` \
    or :meth:`~aerospike.predicates.equals`) using :meth:`where`, then \
    invoked using either :meth:`foreach` or :meth:`results`. The bins returned \
    can be filtered by using :meth:`select`.

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

        :param tuple predicate: the :class:`tuple` produced by one of the :mod:`aerospike.predicates` methods.

        .. note:: Currently, you can assign at most one predicate to the query.


    .. method:: results([policy]) -> list of (key, meta, bins)

        Buffer the records resulting from the query, and return them as a \
        :class:`list` of records.

        :param dict policy: optional query policies :ref:`aerospike_query_policies`.
        :return: a :class:`list` of :ref:`aerospike_record_tuple`.

        .. code-block:: python

            import aerospike
            from aerospike import predicates as p
            import pprint

            config = { 'hosts': [ ('127.0.0.1', 3000)]}
            client = aerospike.client(config).connect()

            pp = pprint.PrettyPrinter(indent=2)
            query = self.client.query('test', 'demo')
            query.select('name', 'age') # matched records return with the values of these bins
            # assuming there is a secondary index on the 'age' bin of test.demo
            query.where(p.equals('age', 40))
            records = query.results( {'timeout':2000})
            pp.pprint(records)

        .. note::

            Queries require a secondary index to exist on the *bin* being queried.


    .. method:: foreach(callback[, policy])

        Invoke the *callback* function for each of the records streaming back \
        from the query.

        :param callback callback: the function to invoke for each record.
        :param dict policy: optional query policies :ref:`aerospike_query_policies`.

        .. seealso:: The :ref:`aerospike_record_tuple`.

        .. code-block:: python

            import aerospike
            from aerospike import predicates as p
            import pprint

            config = { 'hosts': [ ('127.0.0.1', 3000)]}
            client = aerospike.client(config).connect()

            pp = pprint.PrettyPrinter(indent=2)
            query = self.client.query('test', 'demo')
            query.select('name', 'age') # matched records return with the values of these bins
            # assuming there is a secondary index on the 'age' bin of test.demo
            query.where(p.between('age', 20, 30))
            names = []
            def matched_names((key, metadata, bins)):
                pp.pprint(bins)
                names.append(bins['name'])

            query.foreach(matched_names, {'timeout':2000})
            pp.pprint(names)


    .. method:: apply(module, function[, arguments])

        Aggregate the :meth:`results` using a stream `UDF <http://www.aerospike.com/docs/guide/udf.html>`_.

        :param str module: the name of the Lua module.
        :param str function: the name of the Lua function within the *module*.
        :param list arguments: optional arguments to pass to the *function*.

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

                config = { 'hosts': [('127.0.0.1', 3000)] }
                client = aerospike.client(config).connect()

                pp = pprint.PrettyPrinter(indent=2)
                query = self.client.query('test', 'users')
                query.where(p.between('age', 20, 29))
                query.apply('stream_udf', 'group_count', [ 'first_name' ])
                names = query.results()
                pp.pprint(names)


.. _aerospike_query_policies:

Query Policies
--------------

.. object:: policy

    A :class:`dict` of optional query policies which are applicable to :meth:`Query.results` and :meth:`Query.foreach`. See :ref:`aerospike_policies`.

    .. hlist::
        :columns: 1

        * **timeout** maximum time in milliseconds to wait for the operation to complete. Default ``0`` means *do not timeout*.


