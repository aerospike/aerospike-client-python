.. _scan:

.. currentmodule:: aerospike

=================================
Scan Class --- :class:`Scan`
=================================

:class:`Scan`
===============

.. class:: Scan

    The Scan object is used to return all the records in a specified set (which \
    can be ommitted or :py:obj:`None`). A Scan with a :py:obj:`None` set returns all the \
    records in the namespace.

    The scan is invoked using either :meth:`foreach` or :meth:`results`. The \
    bins returned can be filtered using :meth:`select`.

    .. seealso::
        `Scans <http://www.aerospike.com/docs/guide/scan.html>`_ and \
        `Managing Scans <http://www.aerospike.com/docs/operations/manage/scans/>`_.


    .. method:: select(bin1[, bin2[, bin3..]])

        Set a filter on the record bins resulting from :meth:`results` or \
        :meth:`foreach`. If a selected bin does not exist in a record it will \
        not appear in the *bins* portion of that record tuple.


    .. method:: results([policy[, nodename]]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a \
        :class:`list` of records.

        :param dict policy: optional :ref:`aerospike_scan_policies`.
        :param str nodename: optional name of node used to limit the scan to a single node.

        :return: a :class:`list` of :ref:`aerospike_record_tuple`.


        .. code-block:: python

            import aerospike
            import pprint

            pp = pprint.PrettyPrinter(indent=2)
            config = { 'hosts': [ ('127.0.0.1',3000)]}
            client = aerospike.client(config).connect()

            client.put(('test','test','key1'), {'id':1,'a':1},
                policy={'key':aerospike.POLICY_KEY_SEND})
            client.put(('test','test','key2'), {'id':2,'b':2},
                policy={'key':aerospike.POLICY_KEY_SEND})

            scan = client.scan('test', 'test')
            scan.select('id','a','zzz')
            res = scan.results()
            pp.pprint(res)
            client.close()

        .. note::

            We expect to see:

            .. code-block:: python

                [ ( ( 'test',
                      'test',
                      u'key2',
                      bytearray(b'\xb2\x18\n\xd4\xce\xd8\xba:\x96s\xf5\x9ba\xf1j\xa7t\xeem\x01')),
                    { 'gen': 52, 'ttl': 2592000},
                    { 'id': 2}),
                  ( ( 'test',
                      'test',
                      u'key1',
                      bytearray(b'\x1cJ\xce\xa7\xd4Vj\xef+\xdf@W\xa5\xd8o\x8d:\xc9\xf4\xde')),
                    { 'gen': 52, 'ttl': 2592000},
                    { 'a': 1, 'id': 1})]


    .. method:: foreach(callback[, policy[, options[, nodename]]])

        Invoke the *callback* function for each of the records streaming back \
        from the scan.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_scan_policies`.
        :param dict options: the :ref:`aerospike_scan_options` that will apply to the scan.
        :param str nodename: optional name of node used to limit the scan to a single node.

        .. note:: A :ref:`aerospike_record_tuple` is passed as the argument to the callback function.

        .. code-block:: python

            import aerospike
            import pprint

            pp = pprint.PrettyPrinter(indent=2)
            config = { 'hosts': [ ('127.0.0.1',3000)]}
            client = aerospike.client(config).connect()

            client.put(('test','test','key1'), {'id':1,'a':1},
                policy={'key':aerospike.POLICY_KEY_SEND})
            client.put(('test','test','key2'), {'id':2,'b':2},
                policy={'key':aerospike.POLICY_KEY_SEND})

            def show_key((key, meta, bins)):
                print(key)

            scan = client.scan('test', 'test')
            scan_opts = {
              'concurrent': True,
              'nobins': True,
              'priority': aerospike.SCAN_PRIORITY_MEDIUM
            }
            scan.foreach(show_key, options=scan_opts)
            client.close()

        .. note::

            We expect to see:

            .. code-block:: python

                ('test', 'test', u'key2', bytearray(b'\xb2\x18\n\xd4\xce\xd8\xba:\x96s\xf5\x9ba\xf1j\xa7t\xeem\x01'))
                ('test', 'test', u'key1', bytearray(b'\x1cJ\xce\xa7\xd4Vj\xef+\xdf@W\xa5\xd8o\x8d:\xc9\xf4\xde'))

        .. note:: To stop the stream return ``False`` from the callback function.

            .. code-block:: python

                from __future__ import print_function
                import aerospike

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

                scan = client.scan('test','user')
                keys = []
                scan.foreach(limit(100, keys))
                print(len(keys)) # this will be 100 if the number of matching records > 100
                client.close()


.. _aerospike_scan_policies:

Scan Policies
-------------

.. object:: policy

    A :class:`dict` of optional scan policies which are applicable to :meth:`Scan.results` and :meth:`Scan.foreach`. See :ref:`aerospike_policies`.

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
            | Default: ``10000``.
        * **total_timeout**
            | An :class:`int`. Total transaction timeout in milliseconds.
            |
            | The total_timeout is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction.
            |
            | If ``total_timeout`` is not zero and ``total_timeout`` is reached before the transaction completes, the transaction will return error ``AEROSPIKE_ERR_TIMEOUT``. If ``total_timeout`` is zero, there will be no total time limit.
            |
            | Default: ``0``
        * **fail_on_cluster_change** :class:`bool`: Abort the scan if the cluster is not in a stable state. Default: ``False``
        * **durable_delete**
            | A :class:`bool` : If the transaction results in a record deletion, leave a tombstone for the record.
            |
            | This prevents deleted records from reappearing after node failures.
            |
            | Valid for Aerospike Server Enterprise Edition only.
            |
            | Default: ``False`` (do not tombstone deleted records).


.. _aerospike_scan_options:

Scan Options
------------

.. object:: options

    A :class:`dict` of optional scan options which are applicable to :meth:`Scan.foreach`.

    .. hlist::
        :columns: 1

        * **priority** See :ref:`aerospike_scan_constants` for values. Default ``aerospike.SCAN_PRIORITY_AUTO``.
        * **nobins** :class:`bool` whether to return the *bins* portion of the :ref:`aerospike_record_tuple`. Default ``False``.
        * **concurrent** :class:`bool` whether to run the scan concurrently on all nodes of the cluster. Default ``False``.
        * **percent** :class:`int` percentage of records to return from the scan. Default ``100``.

    .. versionadded:: 1.0.39

