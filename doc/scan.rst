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


    .. method:: results([policy]) -> list of (key, meta, bins)

        Buffer the records resulting from the scan, and return them as a \
        :class:`list` of records.

        :param dict policy: optional :ref:`aerospike_scan_policies`.
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


    .. method:: foreach(callback[, policy[, options]])

        Invoke the *callback* function for each of the records streaming back \
        from the scan.

        :param callable callback: the function to invoke for each record.
        :param dict policy: optional :ref:`aerospike_scan_policies`.
        :param dict options: the :ref:`aerospike_scan_options` that will apply \
           to the scan.

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

        * **timeout** maximum time in milliseconds to wait for the operation to complete. Default ``0`` means *do not timeout*.
        * **fail_on_cluster_change** :class:`bool` whether to fail the scan if a change occurs on the cluster. Default ``True``.


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
        * **include_ldt** :class:`bool` whether to include LDT bins with the scan. Default ``False``.
        * **percent** :class:`int` percentage of records to return from the scan. Default ``100``.

    .. versionadded:: 1.0.39

