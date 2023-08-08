.. _aerospike:

*************************************************
:mod:`aerospike` --- Aerospike Client for Python
*************************************************

Overview
========

.. module:: aerospike
    :platform: 64-bit Linux and OS X
    :synopsis: Aerospike client for Python.

``aerospike`` is a package which provides a Python client for Aerospike database clusters.

The Aerospike client enables you to build an application in Python with an
Aerospike cluster as its database. The client manages the connections to the
cluster and handles the transactions performed against it.

Methods
=======

Constructors
------------

Client
^^^^^^

.. py:function:: client(config)

    Creates a new instance of the :class:`Client` class and immediately connects to the cluster.

    See :ref:`client` for more details.

    Internally, this is a wrapper function which calls the constructor for the :class:`Client` class.
    However, the client may also be constructed by calling the constructor directly.

    The client takes on many configuration parameters passed in through a dictionary.

    :param dict config: See :ref:`client_config`.

    :return: an instance of the :class:`Client` class.

    .. versionchanged:: 2.0.0

    Simple example:

    .. code-block:: python

        import aerospike

        # Configure the client to first connect to a cluster node at 127.0.0.1
        # The client will learn about the other nodes in the cluster from the seed node.
        # Also sets a top level policy for read operations
        config = {
            'hosts':    [ ('127.0.0.1', 3000) ],
            'policies': {'read': {'total_timeout': 1000}},
        }
        client = aerospike.client(config)

    Connecting using TLS example:

    .. code-block:: python

        import aerospike
        import sys

        # NOTE: Use of TLS requires Aerospike Enterprise version >= 3.11
        # and client version 2.1.0 or greater
        tls_name = "some-server-tls-name"
        tls_ip = "127.0.0.1"
        tls_port = 4333

        # If tls-name is specified,
        # it must match the tls-name in the node’s server configuration file
        # and match the server’s CA certificate.
        tls_host_tuple = (tls_ip, tls_port, tls_name)
        hosts = [tls_host_tuple]

        # Example configuration which will use TLS with the specifed cafile
        tls_config = {
            "cafile": "/path/to/cacert.pem",
            "enable": True
        }
        try:
            client = aerospike.client({
                "hosts": hosts,
                "tls": tls_config
            })
        except Exception as e:
            print(e)
            print("Failed to connect")
            sys.exit()

Geospatial
^^^^^^^^^^

.. py:function:: geodata([geo_data])

    Helper for creating an instance of the :class:`~aerospike.GeoJSON` class. \
    Used to wrap a geospatial object, such as a point, polygon or circle.

    :param dict geo_data: a :class:`dict` representing the geospatial data.
    :return: an instance of the :py:class:`aerospike.GeoJSON` class.

    .. code-block:: python

        import aerospike

        # Create GeoJSON point using WGS84 coordinates.
        latitude = 45.920278
        longitude = 63.342222
        loc = aerospike.geodata({'type': 'Point',
                                 'coordinates': [longitude, latitude]})

    .. versionadded:: 1.0.54

.. py:function:: geojson([geojson_str])

    Helper for creating an instance of the :class:`~aerospike.GeoJSON` class \
    from a raw GeoJSON :class:`str`.

    :param dict geojson_str: a :class:`str` of raw GeoJSON.
    :return: an instance of the :py:class:`aerospike.GeoJSON` class.

    .. code-block:: python

        import aerospike

        # Create GeoJSON point using WGS84 coordinates.
        loc = aerospike.geojson('{"type": "Point", "coordinates": [-80.604333, 28.608389]}')

    .. versionadded:: 1.0.54

Types
-----

.. py:function:: null()

    A type for distinguishing a server-side null from a Python :py:obj:`None`.
    Replaces the constant ``aerospike.null``.

    :return: a type representing the server-side type ``as_null``.

    .. versionadded:: 2.0.1


.. py:function:: CDTWildcard()

    A type representing a wildcard object. This type may only be used as a comparison value in operations.
    It may not be stored in the database.

    :return: a type representing a wildcard value.

    .. code-block:: python

        import aerospike
        from aerospike_helpers.operations import list_operations as list_ops

        client = aerospike.client({'hosts': [('localhost', 3000)]})
        key = 'test', 'demo', 1

        #  get all values of the form [1, ...] from a list of lists.
        #  For example if list is [[1, 2, 3], [2, 3, 4], [1, 'a']], this operation will match
        #  [1, 2, 3] and [1, 'a']
        operations = [list_ops.list_get_by_value('list_bin', [1, aerospike.CDTWildcard()], aerospike.LIST_RETURN_VALUE)]
        _, _, bins = client.operate(key, operations)

    .. versionadded:: 3.5.0
    .. note:: This requires Aerospike Server 4.3.1.3 or greater

.. py:function:: CDTInfinite()

    A type representing an infinite value. This type may only be used as a comparison value in operations.
    It may not be stored in the database.

    :return: a type representing an infinite value.

    .. code-block:: python

        import aerospike
        from aerospike_helpers.operations import list_operations as list_ops

        client = aerospike.client({'hosts': [('localhost', 3000)]})
        key = 'test', 'demo', 1

        #  get all values of the form [1, ...] from a list of lists.
        #  For example if list is [[1, 2, 3], [2, 3, 4], [1, 'a']], this operation will match
        #  [1, 2, 3] and [1, 'a']
        operations = [list_ops.list_get_by_value_range('list_bin', aerospike.LIST_RETURN_VALUE, [1],  [1, aerospike.CDTInfinite()])]
        _, _, bins = client.operate(key, operations)

    .. versionadded:: 3.5.0
    .. note:: This requires Aerospike Server 4.3.1.3 or greater

Serialization
-------------

.. note::
    See :ref:`Data_Mapping`.

.. note::
    If the client's config dictionary has a serializer and deserializer in the `serialization` tuple, \
    then it will take precedence over the ones from :meth:`set_serializer` and :meth:`set_deserializer`.

.. py:function:: set_serializer(callback)

    Register a user-defined serializer available to all `Client`
    instances.

    :param callable callback: the function to invoke for serialization.


    .. seealso:: To use this function with :meth:`Client.put`, \
        the argument to the serializer parameter should be :const:`aerospike.SERIALIZER_USER`.

    .. code-block:: python

        def my_serializer(val):
            return json.dumps(val)

        aerospike.set_serializer(my_serializer)

    .. versionadded:: 1.0.39

.. py:function:: set_deserializer(callback)

    Register a user-defined deserializer available to all :class:`Client`
    instances.

    Once registered, all read methods (such as :meth:`Client.get`) will run bins containing 'Generic' *as_bytes* \
    of type `AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_
    through this deserializer.

    :param callable callback: the function to invoke for deserialization.

.. py:function:: unset_serializers()

    Deregister the user-defined deserializer/serializer available from :class:`Client`
    instances.

    .. versionadded:: 1.0.53

Example
^^^^^^^
The following example shows the three modes of serialization:

1. Built-in
2. Class-level user functions
3. Instance-level user functions

.. include:: examples/serializer.py
    :code: python

Records ``foo1`` and ``foo2`` should have different encodings from each other since they use different serializers.
(record ``foo3`` uses the same encoding as ``foo2``)
If we read the data for each record using ``aql``, it outputs the following data:

.. code-block:: sql

    aql> select bin from test.demo where PK='foo1'
    +-------------------------------------------------------------+--------+
    | bin                                                         | PK     |
    +-------------------------------------------------------------+--------+
    | 80 04 95 09 00 00 00 00 00 00 00 4B 01 4B 02 4B 03 87 94 2E | "foo1" |
    +-------------------------------------------------------------+--------+
    1 row in set (0.000 secs)

    OK

    aql> select bin from test.demo where PK='foo2'
    +----------------------------+--------+
    | bin                        | PK     |
    +----------------------------+--------+
    | 5B 31 2C 20 32 2C 20 33 5D | "foo2" |
    +----------------------------+--------+
    1 row in set (0.001 secs)

    OK


Logging
-------

.. py:function:: set_log_handler(callback)

    Enables aerospike log handler

    :param optional callable callback: the function used as the logging handler.

    .. note:: The callback function must have the five parameters (level, func, path, line, msg)

        .. code-block:: python

            import aerospike

        from __future__ import print_function
        import aerospike

        aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
        aerospike.set_log_handler(callback)


.. py:function:: set_log_level(log_level)

    Declare the logging level threshold for the log handler.

    :param int log_level: one of the :ref:`aerospike_log_levels` constant values.

Other
-----

.. py:function:: calc_digest(ns, set, key) -> bytearray

    Calculate the digest of a particular key. See: :ref:`aerospike_key_tuple`.

    :param str ns: the namespace in the aerospike cluster.
    :param str set: the set name.
    :param key: the primary key identifier of the record within the set.
    :type key: :class:`str`, :class:`int` or :class:`bytearray`
    :return: a RIPEMD-160 digest of the input tuple.
    :rtype: :class:`bytearray`

    .. code-block:: python

        import aerospike
        import pprint

        digest = aerospike.calc_digest("test", "demo", 1 )
        pp.pprint(digest)

.. _client_config:

Client Configuration
====================

These are the keys and expected values for the ``config`` dictionary passed to :meth:`aerospike.client`.

Only the `hosts` key is required; the rest of the keys are optional.

.. object:: config

    .. hlist::
        :columns: 1

        * **hosts** (:class:`list`)
            A list of tuples identifying a node (or multiple nodes) in the cluster.

            The tuple is in this format: ``(address, port, [tls-name])``

            * address: :class:`str`
            * port: :class:`int`
            * tls-name: :class:`str`

            The client will connect to the first available node in the list called the *seed node*.
            From there, it will learn about the cluster and its partition map.

            If ``tls-name`` is specified, it must match the tls-name specified in the node's \
            server configuration file, as well as the server's CA certificate.

        * **user** (:class:`str`)
            (Optional) A defined user with roles in the cluster. See :meth:`admin_create_user`.
        * **password** (:class:`str`)
            (Optional) The password will be hashed by the client using bcrypt.
        * **lua** (:class:`dict`)
            (Optional) Contains the paths to two types of Lua modules

            * **system_path** (:class:`str`)
                The location of the system modules such as ``aerospike.lua``

                Default: ``/usr/local/aerospike/lua``

            * **user_path** (:class:`str`)
                The location of the user's record and stream UDFs .

                Default: ``./``

        * **policies** (:class:`dict`)
            A :class:`dict` of policies

            * **read** (:class:`dict`)
                Contains :ref:`aerospike_read_policies`.
            * **write** (:class:`dict`)
                Contains :ref:`aerospike_write_policies`.
            * **apply** (:class:`dict`)
                Contains :ref:`aerospike_apply_policies`.
            * **operate** (:class:`dict`)
                Contains :ref:`aerospike_operate_policies`.
            * **remove** (:class:`dict`)
                Contains :ref:`aerospike_remove_policies`.
            * **query** (:class:`dict`)
                Contains :ref:`aerospike_query_policies`.
            * **scan** (:class:`dict`)
                Contains :ref:`aerospike_scan_policies`.
            * **batch** (:class:`dict`)
                Contains :ref:`aerospike_batch_policies`.
            * **info** (:class:`dict`)
                Contains :ref:`aerospike_info_policies`.
            * **admin** (:class:`dict`)
                Contains :ref:`aerospike_admin_policies`.
            * **total_timeout** (:class:`int`)
                **Deprecated**: set this individually in the :ref:`aerospike_policies` dictionaries.

                The default connection timeout in milliseconds

            * **auth_mode**
                The authentication mode with the server.

                See :ref:`auth_mode` for possible values.

                Default: :data:`aerospike.AUTH_INTERNAL`
            * **login_timeout_ms** (:class:`int`)
                Representing the node login timeout in milliseconds.

                Default: ``5000``.
            * **key**
                **Deprecated**: set this individually in the :ref:`aerospike_policies` dictionaries.

                Default key policy.

                See :ref:`POLICY_KEY` for possible values.
            * **exists**
                **Deprecated**: set in the :ref:`aerospike_write_policies` dictionary

                Default exists policy.

                See :ref:`POLICY_EXISTS` for possible values.
            * **max_retries** (:class:`int`)
                **Deprecated**: set this individually in the :ref:`aerospike_policies` dictionaries.

                Representing the number of times to retry a transaction
            * **replica**
                **Deprecated**: set this in one or all of the following policy dictionaries:

                    * :ref:`aerospike_read_policies`
                    * :ref:`aerospike_write_policies`
                    * :ref:`aerospike_apply_policies`
                    * :ref:`aerospike_operate_policies`
                    * :ref:`aerospike_remove_policies`

                Default replica policy.

                See :ref:`POLICY_REPLICA` for possible values.
            * **commit_level**
                **Deprecated**: set this as needed individually in the following policy dictionaries:

                    * :ref:`aerospike_write_policies`
                    * :ref:`aerospike_apply_policies`
                    * :ref:`aerospike_operate_policies`
                    * :ref:`aerospike_remove_policies`

                Default commit level policy.

                See :ref:`POLICY_COMMIT_LEVEL` for possible values.

                .. seealso::
                    `Per-Transaction Consistency Guarantees <http://www.aerospike.com/docs/architecture/consistency.html>`_.

        * **shm** (:class:`dict`)
            Contains optional shared-memory cluster tending parameters

            Shared-memory cluster tending is on if the :class:`dict` is provided. \
            If multiple clients are instantiated and talking to the same cluster the *shm* cluster-tending should be used.

            * **max_nodes** (:class:`int`)
                Maximum number of nodes allowed.

                Pad this value so new nodes can be added without configuration changes.

                Default: ``16``
            * **max_namespaces** (:class:`int`)
                Maximum number of namespaces allowed.

                Pad this value so new namespaces can be added without configuration changes.

                Default: ``8``
            * **takeover_threshold_sec**  (:class:`int`)
                Take over tending if the cluster hasn't been checked for this many seconds

                Default: ``30``
            * **shm_key**
                Explicitly set the shm key for this client.

                If **use_shared_connection** is not set, or set to ``False``, the user must provide a value for this field in order for shared memory to work correctly.

                If, and only if, **use_shared_connection** is set to ``True``, the key will be implicitly evaluated per unique hostname, and can be inspected with :meth:`Client.shm_key` .

                It is still possible to specify a key when using **use_shared_connection** = `True`.

                Default: ``0xA9000000``

                .. seealso::
                    `Shared Memory <https://developer.aerospike.com/client/c/shm>`_

        * **use_shared_connection** (:class:`bool`)
            Indicates whether this instance should share its connection to the Aerospike cluster with other client instances in the same process.

            Default: ``False``
        * **tls** (:class:`dict`)
            Contains optional TLS configuration parameters.

            .. note:: TLS usage requires Aerospike Enterprise Edition. See `TLS <https://www.aerospike.com/docs/guide/security/tls.html>`_.

            * **enable** (:class:`bool`)
                Indicating whether tls should be enabled or not.

                Default: ``False``
            * **cafile** (:class:`str`)
                Path to a trusted CA certificate file.

                By default TLS will use system standard trusted CA certificates
            * **capath** (:class:`str`)
                Path to a directory of trusted certificates.

                See the OpenSSL SSL_CTX_load_verify_locations manual page for more information about the format of the directory.
            * **protocols** (:class:`str`)
                Specifies enabled protocols. This format is the same as Apache's SSLProtocol documented at https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol .

                If not specified the client will use "-all +TLSv1.2".
            * **cipher_suite** (:class:`str`)
                Specifies enabled cipher suites.

                The format is the same as OpenSSL's Cipher List Format documented at https://www.openssl.org/docs/man1.1.1/man1/ciphers.html .

                If not specified, the OpenSSL default cipher suite described in the ciphers documentation will be used. If you are not sure what cipher suite to select, this option is best left unspecified.
            * **keyfile** (:class:`str`)
                Path to the client's key for mutual authentication.

                By default, mutual authentication is disabled.
            * **keyfile_pw** (:class:`str`)
                Decryption password for the client's key for mutual authentication.

                By default, the key is assumed not to be encrypted.
            * **cert_blacklist** (:class:`str`)
                Path to a certificate blacklist file.

                The file should contain one line for each blacklisted certificate. \
                Each line starts with the certificate serial number expressed in hex. \
                Each entry may optionally specify the issuer name of the certificate (serial numbers are only required to be unique per issuer).

                Example records: ``867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=Test Chain CA E2D4B0E570F9EF8E885C065899886461``
            * **certfile** (:class:`str`)
                Path to the client's certificate chain file for mutual authentication.

                By default, mutual authentication is disabled.
            * **crl_check** (:class:`bool`)
                Enable CRL checking for the certificate chain leaf certificate.

                An error occurs if a suitable CRL cannot be found.

                By default CRL checking is disabled.
            * **crl_check_all** (:class:`bool`)
                Enable CRL checking for the entire certificate chain.

                An error occurs if a suitable CRL cannot be found.

                By default CRL checking is disabled.
            * **log_session_info** (:class:`bool`)
                Log session information for each connection.
            * **for_login_only** (:class:`bool`)
                Log session information for each connection.

                Use TLS connections only for login authentication. All other communication with the server will be done with non-TLS connections.

                Default: ``False`` (Use TLS connections for all communication with server.)
        * **send_bool_as** (:class:`int`)
            Configures the client to encode Python booleans as the native Python boolean type, an integer, or the server boolean type.

            Use one of the :ref:`send_bool_as_constants` constant values.

            See :ref:`Data_Mapping` for more information.

            Default: :data:`aerospike.AS_BOOL`
        * **serialization** (:class:`tuple`)
            An optional instance-level `tuple` of ``(serializer, deserializer)``.

            Takes precedence over a class serializer registered with :func:`~aerospike.set_serializer`.
        * **thread_pool_size** (:class:`int`)
            Number of threads in the pool that is used in batch/scan/query commands.

            Default: ``16``
        * **max_socket_idle** (:class:`int`)
            Maximum socket idle in seconds. Connection pools will discard sockets that have been idle longer than the maximum.

            Connection pools are now implemented by a LIFO stack.
            Connections at the tail of the stack will always be the least used.
            These connections are checked for ``max_socket_idle`` once every 30 tend iterations (usually 30 seconds).

            If server's ``proto-fd-idle-ms`` is greater than zero,
            then ``max_socket_idle`` should be at least a few seconds less than the server's ``proto-fd-idle-ms``,
            so the client does not attempt to use a socket that has already been reaped by the server.

            If server's ``proto-fd-idle-ms`` is zero (no reap), then ``max_socket_idle`` should also be zero.
            Connections retrieved from a pool in transactions will not be checked for ``max_socket_idle`` when ``max_socket_idle`` is zero.
            Idle connections will still be trimmed down from peak connections to min connections \
            (``min_conns_per_node`` and ``async_min_conns_per_node``) using a hard-coded 55 second limit in the cluster tend thread.

            Default: ``0``

        * **min_conns_per_node** (:class:`int`)
            Minimum number of synchronous connections allowed per server node. Preallocate minimum
            connections on client node creation.  The client will periodically allocate new connections
            if count falls below min connections.

            Server ``proto-fd-idle-ms`` and client ``max_socket_idle`` should be set to zero (no reap) if
            ``min_conns_per_node`` is greater than zero.  Reaping connections can defeat the purpose
            of keeping connections in reserve for a future burst of activity.

            Default: ``0``
        * **max_conns_per_node** (:class:`int`)
            Maximum number of pipeline connections allowed for each node

            Default: ``100``
        * **max_error_rate** (:class:`int`)
            Maximum number of errors allowed per node per ``error_rate_window`` before backoff algorithm returns :exc:`~aerospike.exception.MaxErrorRateExceeded` for database commands to that node. If ``max_error_rate`` is zero, there is no error limit.

            The counted error types are any error that causes the connection to close (socket errors and client timeouts), server device overload and server timeouts.

            The application should backoff or reduce the transaction load until :exc:`~aerospike.exception.MaxErrorRateExceeded` stops being returned.

            Default: ``100``
        * **error_rate_window** (:class:`int`)
            The number of cluster tend iterations that defines the window for ``max_error_rate``. One tend iteration is defined as ``tend_interval`` plus the time to tend all nodes. At the end of the window, the error count is reset to zero and backoff state is removed on all nodes.

            Default: ``1``
        * **tend_interval** (:class:`int`)
            Polling interval in milliseconds for tending the cluster

            Default: ``1000``
        * **compression_threshold** (:class:`int`)
            **Deprecated**: set in the :ref:`aerospike_write_policies` dictionary

            Compress data for transmission if the object size is greater than a given number of bytes

            Default: ``0``, meaning 'never compress'
        * **cluster_name** (:class:`str`)
            Only server nodes matching this name will be used when determining the cluster name.
        * **rack_id** (:class:`int`)
            Rack id where this client instance resides.

            ``rack_aware``, ``POLICY_REPLICA_PREFER_RACK`` and server rack configuration must also be set to enable this functionality.

            Default: ``0``

        * **rack_ids** (:class:`list`)
            List of preferred racks in order of preference. If ``rack_ids`` is set, ``rack_id`` is ignored.

            ``rack_aware``, ``POLICY_REPLICA_PREFER_RACK`` and server rack configuration must also be set to enable this functionality.

        * **rack_aware** (:class:`bool`)
            Track server rack data.

            This is useful for:

                - Directing read operations to run on the same rack as the client.
                - Lowering cloud provider costs when nodes are distributed across different availability zones (represented as racks).

            In order to enable this functionality:

            - ``rack_id`` needs to be set to the local rack's ID
            - The client config's :ref:`aerospike_read_policies` needs to be set to :data:`POLICY_REPLICA_PREFER_RACK`
            - The server rack configuration must be configured.

            Default: ``False``
        * **use_services_alternate** (:class:`bool`)
            Flag to signify if "services-alternate" should be used instead of "services".

            Default: ``False``
        * **connect_timeout** (:class:`int`)
            Initial host connection timeout in milliseconds. The timeout when opening a connection to the server host for the first time.

            Default: ``1000``.
        * **fail_if_not_connected** (:class:`bool`)
            Flag to signify fail on cluster init if seed node and all peers are not reachable.

            Default: ``True``

Constants
=========

Policy Options
--------------

.. _POLICY_COMMIT_LEVEL:

Commit Level Policy Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the number of replicas required to be successfully committed before returning success in a write operation to
provide the desired consistency guarantee.

.. data:: POLICY_COMMIT_LEVEL_ALL

    Return success only after successfully committing all replicas

.. data:: POLICY_COMMIT_LEVEL_MASTER

    Return success after successfully committing the master replica


.. _POLICY_READ_MODE_AP:

AP Read Mode Policy Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Read policy for AP (availability) namespaces.

.. data:: POLICY_READ_MODE_AP_ONE

    Involve single node in the read operation.

.. data:: POLICY_READ_MODE_AP_ALL

    Involve all duplicates in the read operation.

.. versionadded:: 3.7.0

.. _POLICY_READ_MODE_SC:

SC Read Mode Policy Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Read policy for SC (strong consistency) namespaces.

.. data:: POLICY_READ_MODE_SC_SESSION

    Ensures this client will only see an increasing sequence of record versions. Server only reads from master.

.. data:: POLICY_READ_MODE_SC_LINEARIZE

    Ensures ALL clients will only see an increasing sequence of record versions. Server only reads from master.

.. versionadded:: 3.7.0

.. data:: POLICY_READ_MODE_SC_ALLOW_REPLICA

    Server may read from master or any full (non-migrating) replica. Increasing sequence of record versions is not guaranteed.

.. data:: POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE

    Server may read from master or any full (non-migrating) replica or from unavailable partitions. Increasing sequence of record versions is not guaranteed.

.. _POLICY_EXISTS:

Existence Policy Options
^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the behavior for writing the record depending whether or not it exists.

.. data:: POLICY_EXISTS_CREATE

    Only create a record given it doesn't exist

.. data:: POLICY_EXISTS_CREATE_OR_REPLACE

    Replace a record completely if it exists, otherwise create it

.. data:: POLICY_EXISTS_IGNORE

    Update a record if it exists, otherwise create it

.. data:: POLICY_EXISTS_REPLACE

    Only replace a record completely if it exists

.. data:: POLICY_EXISTS_UPDATE

    Only update a record if it exists

.. _POLICY_GEN:

Generation Policy Options
^^^^^^^^^^^^^^^^^^^^^^^^^

Specifies the behavior of record modifications with regard to the generation value.

.. data:: POLICY_GEN_IGNORE

    Write a record regardless of generation

.. data:: POLICY_GEN_EQ

    Write a record only if generations are equal

.. data:: POLICY_GEN_GT

    Write a record only if local generation is greater than remote generation

.. _POLICY_KEY:

Key Policy Options
^^^^^^^^^^^^^^^^^^

Specifies the behavior for whether keys or digests should be sent to the cluster.

.. data:: POLICY_KEY_DIGEST

    Calculate the digest on the client-side and send it to the server

.. data:: POLICY_KEY_SEND

    Send the key in addition to the digest. This policy causes a write operation to store the key on the server

.. _POLICY_REPLICA:

Replica Options
^^^^^^^^^^^^^^^

Specifies which partition replica to read from.

.. data:: POLICY_REPLICA_SEQUENCE

    Always try node containing master partition first.

    If connection fails and the client is configured to retry, it will try the node containing prole partition.
    Currently restricted to master and one prole.

.. data:: POLICY_REPLICA_MASTER

    Read from the partition master replica node

.. data:: POLICY_REPLICA_ANY

    Distribute reads across nodes containing key's master and replicated partition in round-robin fashion.

    Currently restricted to master and one prole.

.. data:: POLICY_REPLICA_PREFER_RACK

    Try node on the same rack as the client first.

    If there are no nodes on the same rack, use :data:`POLICY_REPLICA_SEQUENCE` instead.

.. _TTL_CONSTANTS:

TTL Constants
-------------

Specifies the TTL constants.

.. data:: TTL_NAMESPACE_DEFAULT

    Use the namespace default TTL.

.. data:: TTL_NEVER_EXPIRE

    Set TTL to never expire.

.. data:: TTL_DONT_UPDATE

    Do not change the current TTL of the record.

.. _auth_mode:

Auth Mode Constants
-------------------

Specifies the type of authentication to be used when communicating with the server.

.. data:: AUTH_INTERNAL

    Use internal authentication only.

    Hashed password is stored on the server.
    Do not send clear password.

.. data:: AUTH_EXTERNAL

    Use external authentication (like LDAP).

    Specific external authentication is configured on server.
    If TLS defined, send clear password on node login via TLS.

    Throw exception if TLS is not defined.

.. data:: AUTH_EXTERNAL_INSECURE

    Use external authentication (like LDAP).

    Specific external authentication is configured on server.
    Send clear password on node login whether or not TLS is defined.

    .. warning::
        This mode should only be used for testing purposes because it is not secure authentication.

.. _aerospike_job_constants:

Job Constants
--------------

.. data:: JOB_SCAN

    Scan job type argument for the module parameter of :meth:`Client.job_info`

.. data:: JOB_QUERY

    Query job type argument for the module parameter of :meth:`Client.job_info`

.. _aerospike_job_constants_status:

Job Statuses
------------

.. data:: JOB_STATUS_UNDEF

.. data:: JOB_STATUS_INPROGRESS

.. data:: JOB_STATUS_COMPLETED

.. versionadded:: 1.0.50

.. _aerospike_serialization_constants:

Serialization Constants
-----------------------

.. data:: SERIALIZER_USER

    Use a user-defined serializer to handle unsupported types. Must have \
    been registered for the aerospike class or configured for the Client object

.. data:: SERIALIZER_NONE

    Do not serialize bins whose data type is unsupported (default)

.. versionadded:: 1.0.47

.. _send_bool_as_constants:

Send Bool Constants
-------------------

Specifies how the Python client will write Python booleans.

.. data:: INTEGER

    Write Python Booleans as integers.

.. data:: AS_BOOL

    Write Python Booleans as ``as_bools``.

    This is the Aerospike server's boolean type.

List
----

.. _aerospike_list_write_flag:

List Write Flags
^^^^^^^^^^^^^^^^

Flags used by list write flag.

.. data:: LIST_WRITE_DEFAULT

    Default. Allow duplicate values and insertions at any index.

.. data:: LIST_WRITE_ADD_UNIQUE

    Only add unique values.

.. data:: LIST_WRITE_INSERT_BOUNDED

    Enforce list boundaries when inserting. Do not allow values to be inserted at index outside current list boundaries.

    .. note:: Requires server version >= 4.3.0

.. data:: LIST_WRITE_NO_FAIL

    Do not raise error if a list item fails due to write flag constraints (always succeed).

    .. note:: Requires server version >= 4.3.0

.. data:: LIST_WRITE_PARTIAL

    Allow other valid list items to be committed if a list item fails due to write flag constraints.

.. _list_return_types:

List Return Types
^^^^^^^^^^^^^^^^^

Return types used by various list operations.

.. data:: LIST_RETURN_NONE

    Do not return any value.

.. data:: LIST_RETURN_INDEX

    Return key index order.

.. data:: LIST_RETURN_REVERSE_INDEX

    Return reverse key order.

.. data:: LIST_RETURN_RANK

    Return value order.

.. data:: LIST_RETURN_REVERSE_RANK

    Return reverse value order.

.. data:: LIST_RETURN_COUNT

    Return count of items selected.

.. data:: LIST_RETURN_VALUE

    Return value for single key read and value list for range read.

.. _aerospike_list_order:

List Order
^^^^^^^^^^

Flags used by list order.

.. data:: LIST_UNORDERED

    List is not ordered. This is the default.

.. data:: LIST_ORDERED

    Ordered list.

.. _aerospike_list_sort_flag:

List Sort Flags
^^^^^^^^^^^^^^^

Flags used by list sort.

.. data:: aerospike.LIST_SORT_DEFAULT

    Default. Preserve duplicates when sorting the list.

.. data:: aerospike.LIST_SORT_DROP_DUPLICATES

    Drop duplicate values when sorting the list.

Maps
----

.. _aerospike_map_write_flag:

Map Write Flag
^^^^^^^^^^^^^^

Flags used by map write flag.

.. note:: Requires server version >= 4.3.0

.. data:: MAP_WRITE_FLAGS_DEFAULT

    Default. Allow create or update.

.. data:: MAP_WRITE_FLAGS_CREATE_ONLY

    If the key already exists, the item will be denied. If the key does not exist, a new item will be created.

.. data:: MAP_WRITE_FLAGS_UPDATE_ONLY

    If the key already exists, the item will be overwritten. If the key does not exist, the item will be denied.

.. data:: MAP_WRITE_FLAGS_NO_FAIL

    Do not raise error if a map item is denied due to write flag constraints (always succeed).

.. data:: MAP_WRITE_FLAGS_PARTIAL

    Allow other valid map items to be committed if a map item is denied due to write flag constraints.

.. _aerospike_map_write_mode:

Map Write Mode
^^^^^^^^^^^^^^

Flags used by map *write mode*.

.. note:: This should only be used for Server version < 4.3.0

.. data:: MAP_UPDATE

    Default. Allow create or update.

.. data:: MAP_CREATE_ONLY

    If the key already exists, the item will be denied. If the key does not exist, a new item will be created.

.. data:: MAP_UPDATE_ONLY

    If the key already exists, the item will be overwritten. If the key does not exist, the item will be denied.

.. _aerospike_map_order:

Map Order
^^^^^^^^^

Flags used by map order.

.. data:: MAP_UNORDERED

    Map is not ordered. This is the default.

.. data:: MAP_KEY_ORDERED

    Order map by key.

.. data:: MAP_KEY_VALUE_ORDERED

    Order map by key, then value.

.. _map_return_types:

Map Return Types
^^^^^^^^^^^^^^^^

Return types used by various map operations.

.. data:: MAP_RETURN_NONE

    Do not return any value.

.. data:: MAP_RETURN_INDEX

    Return key index order.

.. data:: MAP_RETURN_REVERSE_INDEX

    Return reverse key order.

.. data:: MAP_RETURN_RANK

    Return value order.

.. data:: MAP_RETURN_REVERSE_RANK

    Return reserve value order.

.. data:: MAP_RETURN_COUNT

    Return count of items selected.

.. data:: MAP_RETURN_KEY

    Return key for single key read and key list for range read.

.. data:: MAP_RETURN_VALUE

    Return value for single key read and value list for range read.

.. data:: MAP_RETURN_KEY_VALUE

    Return key/value items.

    Note that key/value pairs will be returned as a list of keys and values next to each other:

        ``[key1, value1, key2, value2, ...]``

.. data:: MAP_RETURN_EXISTS

    Return true if count of items selected > 0.

.. data:: MAP_RETURN_UNORDERED_MAP

    Return unordered map.

    For the Python client, this return type returns the same results as :data:`aerospike.MAP_RETURN_ORDERED_MAP`.

.. data:: MAP_RETURN_ORDERED_MAP

    Return ordered map.

Bitwise
-------

.. _aerospike_bitwise_write_flag:

Bitwise Write Flags
^^^^^^^^^^^^^^^^^^^

.. data:: BIT_WRITE_DEFAULT

    Allow create or update (default).

.. data:: BIT_WRITE_CREATE_ONLY

    If bin already exists the operation is denied. Otherwise the bin is created.

.. data:: BIT_WRITE_UPDATE_ONLY

    If bin does not exist the operation is denied. Otherwise the bin is updated.

.. data:: BIT_WRITE_NO_FAIL

    Do not raise error if operation failed.

.. data:: BIT_WRITE_PARTIAL

    Allow other valid operations to be committed if this operation is denied due to
    flag constraints. i.e. If the number of bytes from the offset to the end of the existing
    Bytes bin is less than the specified number of bytes, then only apply operations
    from the offset to the end.

.. versionadded:: 3.9.0

.. _aerospike_bitwise_resize_flag:

Bitwise Resize Flags
^^^^^^^^^^^^^^^^^^^^

.. data:: BIT_RESIZE_DEFAULT

    Add/remove bytes from the end (default).

.. data:: BIT_RESIZE_FROM_FRONT

    Add/remove bytes from the front.

.. data:: BIT_RESIZE_GROW_ONLY

    Only allow the bitmap size to increase.

.. data:: BIT_RESIZE_SHRINK_ONLY

    Only allow the bitmap size to decrease.

.. _aerospike_bitwise_overflow:

.. versionadded:: 3.9.0

Bitwise Overflow
^^^^^^^^^^^^^^^^

.. data:: BIT_OVERFLOW_FAIL

    Operation will fail on overflow/underflow.

.. data:: BIT_OVERFLOW_SATURATE

    If add or subtract ops overflow/underflow, set to max/min value.
    Example: MAXINT + 1 = MAXINT.

.. data:: BIT_OVERFLOW_WRAP

    If add or subtract ops overflow/underflow, wrap the value.
    Example: MAXINT + 1 = MININT.

.. versionadded:: 3.9.0

.. _aerospike_hll_write_flags:

HyperLogLog Write Flags
-----------------------

.. data:: HLL_WRITE_DEFAULT

    Default. Allow create or update.

.. data:: HLL_WRITE_CREATE_ONLY

    If the bin already exists, the operation will be denied. If the bin does not exist, a new bin will be created.

.. data:: HLL_WRITE_UPDATE_ONLY

    If the bin already exists, the bin will be overwritten. If the bin does not exist, the operation will be denied.

.. data:: HLL_WRITE_NO_FAIL

    Do not raise error if operation is denied.

.. data:: HLL_WRITE_ALLOW_FOLD

    Allow the resulting set to be the minimum of provided index bits. For intersect_counts and similarity, allow the usage of less precise HLL algorithms when minhash bits of all participating sets do not match.

.. versionadded:: 3.11.0

.. _aerospike_expression_write_flags:

Write Expression Flags
----------------------
Flags used by :class:`~aerospike_helpers.operations.expression_operations.expression_write`.

.. data:: EXP_WRITE_DEFAULT

    Default. Allow create or update.

.. data:: EXP_WRITE_CREATE_ONLY

    If bin does not exist, a new bin will be created.
    If bin exists, the operation will be denied.
    If bin exists, fail with BinExistsError
    when EXP_WRITE_POLICY_NO_FAIL is not set.

.. data:: EXP_WRITE_UPDATE_ONLY

    If bin exists, the bin will be overwritten.
    If bin does not exist, the operation will be denied.
    If bin does not exist, fail with BinNotFound
    when EXP_WRITE_POLICY_NO_FAIL is not set.

.. data:: EXP_WRITE_ALLOW_DELETE

    If expression results in nil value, then delete the bin. Otherwise, return
    OpNotApplicable when EXP_WRITE_POLICY_NO_FAIL is not set.

.. data:: EXP_WRITE_POLICY_NO_FAIL

    Do not raise error if operation is denied.

.. data:: EXP_WRITE_EVAL_NO_FAIL

    Ignore failures caused by the expression resolving to unknown or a non-bin type.

.. _aerospike_expression_read_flags:

Read Expression Flags
---------------------
Flags used by :class:`~aerospike_helpers.operations.expression_operations.expression_read`.

.. data:: EXP_READ_DEFAULT

    Default.

.. data:: EXP_READ_EVAL_NO_FAIL

    Ignore failures caused by the expression resolving to unknown or a non-bin type.

.. _aerospike_bin_types:

Bin Types
---------

.. data:: AS_BYTES_UNDEF

    (int): 0

.. data:: AS_BYTES_INTEGER

    (int): 1

.. data:: AS_BYTES_DOUBLE

    (int): 2

.. data:: AS_BYTES_STRING

    (int): 3

.. data:: AS_BYTES_BLOB

    (int): 4

.. data:: AS_BYTES_JAVA

    (int): 7

.. data:: AS_BYTES_CSHARP

    (int): 8

.. data:: AS_BYTES_PYTHON

    (int): 9

.. data:: AS_BYTES_RUBY

    (int): 10

.. data:: AS_BYTES_PHP

    (int): 11

.. data:: AS_BYTES_ERLANG

    (int): 12

.. data:: AS_BYTES_BOOL

    (int): 17

.. data:: AS_BYTES_HLL

    (int): 18

.. data:: AS_BYTES_MAP

    (int): 19

.. data:: AS_BYTES_LIST

    (int): 20

.. data:: AS_BYTES_GEOJSON

    (int): 23

.. data:: AS_BYTES_TYPE_MAX

    (int): 24


.. _aerospike_misc_constants:

Miscellaneous
-------------

.. data:: __version__

    A :class:`str` containing the module's version.

    .. versionadded:: 1.0.54

.. data:: UDF_TYPE_LUA

    UDF type is LUA (which is the only UDF type).

.. data:: INDEX_STRING

    An index whose values are of the aerospike string data type.

.. data:: INDEX_NUMERIC

    An index whose values are of the aerospike integer data type.

.. data:: INDEX_GEO2DSPHERE

    An index whose values are of the aerospike GetJSON data type.

.. seealso:: `Data Types <https://docs.aerospike.com/server/guide/data-types/overview>`_.

.. data:: INDEX_TYPE_DEFAULT

    Index a bin that doesn't contain a complex data type.

.. data:: INDEX_TYPE_LIST

    Index a bin whose contents is an aerospike list.

.. data:: INDEX_TYPE_MAPKEYS

    Index the keys of a bin whose contents is an aerospike map.

.. data:: INDEX_TYPE_MAPVALUES

    Index the values of a bin whose contents is an aerospike map.

.. _aerospike_log_levels:

Log Level
---------

.. data:: LOG_LEVEL_TRACE

.. data:: LOG_LEVEL_DEBUG

.. data:: LOG_LEVEL_INFO

.. data:: LOG_LEVEL_WARN

.. data:: LOG_LEVEL_ERROR

.. data:: LOG_LEVEL_OFF


.. _aerospike_privileges:

Privileges
----------

Permission codes define the type of permission granted for a user's role.

.. data:: PRIV_READ

    The user is granted read access.

.. data:: PRIV_WRITE

    The user is granted write access.

.. data:: PRIV_READ_WRITE

    The user is granted read and write access.

.. data:: PRIV_READ_WRITE_UDF

    The user is granted read and write access, and the ability to invoke UDFs.

.. data:: PRIV_SYS_ADMIN

    The user is granted the ability to perform system administration operations. Global scope only.

.. data:: PRIV_USER_ADMIN

    The user is granted the ability to perform user administration operations. Global scope only.

.. data:: PRIV_DATA_ADMIN

    User can perform systems administration functions on a database that do not involve user administration. Examples include setting dynamic server configuration. Global scope only.

.. data:: PRIV_TRUNCATE

    User can truncate data only. Requires server 6.0+

.. data:: PRIV_UDF_ADMIN

    User can perform user defined function(UDF) administration actions. Examples include create/drop UDF. Global scope only. Global scope only. Requires server version 6.0+

.. data:: PRIV_SINDEX_ADMIN

    User can perform secondary index administration actions. Examples include create/drop index. Global scope only. Requires server version 6.0+


.. _regex_constants:

Regex Flag Values
------------------
Flags used by the :class:`aerospike_operation_helpers.expressions.base.CmpRegex` Aerospike expression.
See :ref:`aerospike_operation_helpers.expressions` for more information.

.. data:: REGEX_NONE

    Use default behavior.

.. data:: REGEX_ICASE

    Do not differentiate case.

.. data:: REGEX_EXTENDED

    Use POSIX Extended Regular Expression syntax when interpreting regex.

.. data:: REGEX_NOSUB

    Do not report position of matches.

.. data:: REGEX_NEWLINE

    Match-any-character operators don't match a newline.
