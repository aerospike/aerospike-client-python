.. _aerospike:

*************************************************
:mod:`aerospike` --- Aerospike Client for Python
*************************************************

.. module:: aerospike
    :platform: 64-bit Linux and OS X
    :synopsis: Aerospike client for Python.

The Aerospike client enables you to build an application in Python with an
Aerospike cluster as its database. The client manages the connections to the
cluster and handles the transactions performed against it.

.. rubric:: Data Model

At the top is the ``namespace``, a container that has one set of policy rules
for all its data, and is similar to the *database* concept in an RDBMS, only
distributed across the cluster. A namespace is subdivided into ``sets``,
similar to *tables*.

Pairs of key-value data called ``bins`` make up ``records``, similar to
*columns* of a *row* in a standard RDBMS. Aerospike is schema-less, meaning
that you do not need to define your bins in advance.

Records are uniquely identified by their key, and record metadata is contained
in an in-memory primary index.

.. seealso::
    `Architecture Overview <http://www.aerospike.com/docs/architecture/index.html>`_
    and `Aerospike Data Model
    <http://www.aerospike.com/docs/architecture/data-model.html>`_ for more
    information about Aerospike.


Methods
=======

.. py:function:: client(config)

    Creates a new instance of the Client class. This client can
    :meth:`Client.connect` to the cluster and perform operations
    against it, such as :meth:`Client.put` and
    :meth:`Client.get` records.

    This is a wrapper function which calls the constructor for the :class:`Client` class.
    The client may also be constructed by calling the constructor directly.

    :param dict config: the client's configuration.

        .. hlist::
            :columns: 1

            * **hosts** a required :class:`list` of (address, port, [tls-name]) tuples identifying a node (or multiple nodes) in the cluster. 
                | The client will connect to the first available node in the list, the *seed node*, \ 
                  and will learn about the cluster and partition map from it. If tls-name is specified, it must match the tls-name specified in the node's \
                 server configuration file and match the server's CA certificate.

                .. note:: TLS usage requires Aerospike Enterprise Edition

            * **lua** an optional :class:`dict` containing the paths to two types of Lua modules
                * **system_path** 
                    | The location of the system modules such as ``aerospike.lua`` 
                    | Default: ``/usr/local/aerospike/lua``
                * **user_path** 
                    | The location of the user's record and stream UDFs . 
                    | Default: ``./``
            * **policies** a :class:`dict` of policies
                * **read** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_read_policies`.
                * **write** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_write_policies`.
                * **apply** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_apply_policies`.
                * **operate** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_operate_policies`.
                * **remove** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_remove_policies`.
                * **query** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_query_policies`.
                * **scan** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_scan_policies`.
                * **batch** (:class:`dict`) 
                    | A dictionary containing :ref:`aerospike_batch_policies`.
                * **total_timeout** default connection timeout in milliseconds 
                    | **Deprecated**: set this individually in the :ref:`aerospike_polices` dictionaries.
                * **auth_mode** 
                    | A value of :ref:`auth_mode` defining how the authentication mode with the server, such as :data:`aerospike.AUTH_INTERNAL`.
                    | Default: :data:`aerospike.AUTH_INTERNAL`
                * **login_timeout_ms** (:class:`int`) 
                    | Representing the node login timeout in milliseconds. 
                    | Default: ``5000``.
                * **key** default key policy
                    | **Deprecated**: set this individually in the :ref:`aerospike_polices` dictionaries.
                * **exists** default exists policy
                    | **Deprecated**: set in the :ref:`aerospike_write_policies` dictionary
                * **max_retries** representing the number of times to retry a transaction 
                    | **Deprecated**: set this individually in the :ref:`aerospike_polices` dictionaries.
                * **replica** default replica policy
                    | **Deprecated**: set this in one or all of the other policies' :ref:`aerospike_read_policies`, :ref:`aerospike_write_policies`, :ref:`aerospike_apply_policies`, :ref:`aerospike_operate_policies`, :ref:`aerospike_remove_policies` dictionaries.
                * **commit_level** default commit level policy
                    | **Deprecated**: set this as needed individually in the :ref:`aerospike_write_policies`, :ref:`aerospike_apply_policies`, :ref:`aerospike_operate_policies`, :ref:`aerospike_remove_policies` dictionaries.
            * **shm** a :class:`dict` with optional shared-memory cluster tending parameters
                | Shared-memory cluster tending is on if the :class:`dict` is provided. \
                  If multiple clients are instantiated talking to the same cluster the *shm* cluster-tending should be used.

                * **max_nodes** (:class:`int`)
                    | Maximum number of nodes allowed. Pad so new nodes can be added without configuration changes 
                    | Default: ``16``
                * **max_namespaces** (:class:`int`)
                    | Similarly pad 
                    | Default: ``8``
                * **takeover_threshold_sec**  (:class:`int`)
                    | Take over tending if the cluster hasn't been checked for this many seconds 
                    | Default: ``30``
                * **shm_key** 
                    | Explicitly set the shm key for this client.
                    | If **use_shared_connection** is not set, or set to ``False``, the user must provide a value for this field in order for shared memory to work correctly.
                    | If , and only if, **use_shared_connection** is set to ``True``, the key will be implicitly evaluated per unique hostname, and can be inspected with :meth:`Client.shm_key` .
                    | It is still possible to specify a key when using **use_shared_connection** = `True`.
                    | default: ``0xA8000000``
            * **use_shared_connection** (:class:`bool`)
                | Indicating whether this instance should share its connection to the Aerospike cluster with other client instances in the same process. 
                | Default: ``False``
            * **tls** a :class:`dict` of optional TLS configuration parameters.
            
                .. note:: TLS usage requires Aerospike Enterprise Edition

                * **enable** (:class:`bool`)
                    | Indicating whether tls should be enabled or not. 
                    | Default: ``False``
                * **cafile** (:class:`str`)
                    | Path to a trusted CA certificate file. By default TLS will use system standard trusted CA certificates
                * **capath** (:class:`str`)
                    | Path to a directory of trusted certificates. See the OpenSSL SSL_CTX_load_verify_locations manual page for more information about the format of the directory.
                * **protocols** (:class:`str`)
                    | Specifies enabled protocols. This format is the same as Apache's SSLProtocol documented at https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol . 
                    | If not specified the client will use "-all +TLSv1.2".
                * **cipher_suite** (:class:`str`)
                    | Specifies enabled cipher suites. The format is the same as OpenSSL's Cipher List Format documented at https://www.openssl.org/docs/manmaster/apps/ciphers.html .
                    | If not specified the OpenSSL default cipher suite described in the ciphers documentation will be used. If you are not sure what cipher suite to select this option is best left unspecified 
                * **keyfile** (:class:`str`)
                    | Path to the client's key for mutual authentication. By default mutual authentication is disabled.
                * **keyfile_pw** (:class:`str`)
                    | Decryption password for the client's key for mutual authentication. By default the key is assumed not to be encrypted.
                * **cert_blacklist** (:class:`str`)
                    | Path to a certificate blacklist file. The file should contain one line for each blacklisted certificate. Each line starts with the certificate serial number expressed in hex. Each entry may optionally specify the issuer name of the certificate (serial numbers are only required to be unique per issuer). Example records: 867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=Test Chain CA E2D4B0E570F9EF8E885C065899886461
                * **certfile** (:class:`str`)
                    | Path to the client's certificate chain file for mutual authentication. By default mutual authentication is disabled.
                * **crl_check** (:class:`bool`)
                    | Enable CRL checking for the certificate chain leaf certificate. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
                * **crl_check_all** (:class:`bool`)
                    | Enable CRL checking for the entire certificate chain. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
                * **log_session_info** (:class:`bool`)
                    | Log session information for each connection.
                * **for_login_only** (:class:`bool`)
                    | Log session information for each connection. Use TLS connections only for login authentication. All other communication with the server will be done with non-TLS connections.
                    | Default: ``False`` (Use TLS connections for all communication with server.)
            * **send_bool_as** an optional :class:`int` that configures the client to write Python booleans as PY_BYTES_BLOB, integer, or the new server boolean type.
                | One of the :ref:`send_bool_as_constants` constant values.
                | Example: :code:`{"send_bool_as", aerospike.aerospike.PY_BYTES}`
                | See :ref:`Data_Mapping` for more information.
                | Default: aerospike.PY_BYTES
            * **serialization** an optional instance-level `tuple` of (serializer, deserializer). 
                | Takes precedence over a class serializer registered with :func:`~aerospike.set_serializer`.
            * **thread_pool_size** (:class:`int`) 
                | Number of threads in the pool that is used in batch/scan/query commands. 
                | Default: ``16``
            * **max_socket_idle** (:class:`int`)
                | Maximum socket idle time in seconds.  Connection pools will discard sockets that have been idle longer than the maximum. \
                  The value is limited to 24 hours (86400). It's important to set this value to a few seconds less than the server's proto-fd-idle-ms \
                 (default 60000 milliseconds, or 1 minute), so the client does not attempt to use a socket that has already been reaped by the server.
                | Default: ``0`` seconds (disabled) for non-TLS connections, 55 seconds for TLS connections
            * **max_conns_per_node** (:class:`int`)
                | Maximum number of pipeline connections allowed for each node 
            * **tend_interval** (:class:`int`)
                | Polling interval in milliseconds for tending the cluster 
                | Default: ``1000``
            * **compression_threshold** (:class:`int`)
                | Compress data for transmission if the object size is greater than a given number of bytes 
                | Default: ``0``, meaning 'never compress' 
                | **Deprecated**, set this in the 'write' policy dictionary.
            * **cluster_name** (:class:`str`)
                | Only server nodes matching this name will be used when determining the cluster name.
            * **rack_id** (:class:`int`)
                | Rack id where this client instance resides.
                | In order to enable this functionality, the `rack_aware` needs to be set to true, the :ref:`aerospike_read_policies` `replica` needs to be set to :data:`POLICY_REPLICA_PREFER_RACK`. \
                  The server rack configuration must also be configured.
                |
                | Default: ``0``
            * **rack_aware** (:class:`bool`)
                | Track server rack data. This is useful when directing read operations to run on the same rack as the client.
                | This is useful to lower cloud provider costs when nodes are distributed across different availability zones (represented as racks).
                | In order to enable this functionality, the `rack_id` needs to be set to local rack, the `read policy` `replica` needs to be set to :data:`POLICY_REPLICA_PREFER_RACK`. \
                  The server rack configuration must also be configured.
                |
                | Default: ``False``
            * **use_services_alternate** (:class:`bool`)
                | Flag to signify if "services-alternate" should be used instead of "services"
                |
                | Default: ``False``
            * **connect_timeout** (:class:`int`) 
                | Initial host connection timeout in milliseconds. The timeout when opening a connection to the server host for the first time.
                | Default: ``1000``.


    :return: an instance of the :class:`Client` class.

    .. seealso::
        `Shared Memory <https://www.aerospike.com/docs/client/c/usage/shm.html>`_ and `Per-Transaction Consistency Guarantees <http://www.aerospike.com/docs/architecture/consistency.html>`_.

    .. code-block:: python

        import aerospike

        # configure the client to first connect to a cluster node at 127.0.0.1
        # the client will learn about the other nodes in the cluster from the
        # seed node.
        # in this configuration shared-memory cluster tending is turned on,
        # which is appropriate for a multi-process context, such as a webserver
        config = {
            'hosts':    [ ('127.0.0.1', 3000) ],
            'policies': {'read': {total_timeout': 1000}},
            'shm':      { }}
        client = aerospike.client(config)

    .. versionchanged:: 2.0.0


    .. code-block:: python

        import aerospike
        import sys

        # NOTE: Use of TLS Requires Aerospike Enterprise Server Version >= 3.11 and Python Client version 2.1.0 or greater
        # To view Instructions for server configuration for TLS see https://www.aerospike.com/docs/guide/security/tls.html
        tls_name = "some-server-tls-name"
        tls_ip = "127.0.0.1"
        tls_port = 4333

        # If tls-name is specified, it must match the tls-name specified in the node’s server configuration file
        # and match the server’s CA certificate.
        tls_host_tuple = (tls_ip, tls_port, tls_name)
        hosts = [tls_host_tuple]

        # Example configuration which will use TLS with the specifed cafile
        tls_config = {
            "cafile": "/path/to/cacert.pem",
            "enable": True
        }

        client = aerospike.client({
            "hosts": hosts,
            "tls": tls_config
        })
        try:
            client.connect()
        except Exception as e:
            print(e)
            print("Failed to connect")
            sys.exit()

        key = ('test', 'demo', 1)
        client.put(key, {'aerospike': 'aerospike'})
        print(client.get(key))

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

        client = aerospike.client({'hosts': [('localhost', 3000)]}).connect()
        key = 'test', 'demo', 1

        #  get all values of the form [1, ...] from a list of lists.
        #  For example if list is [[1, 2, 3], [2, 3, 4], [1, 'a']], this operation will match
        #  [1, 2, 3] and [1, 'a']
        operations = [list_ops.list_get_by_value('list_bin', [1, aerospike.CDTWildcard()], aerospike.LIST_RETURN_VALUE)]
        _, _, bins = client.operate(key, operations)

    .. versionadded:: 3.5.0
    .. note:: This requires Aerospike Server 4.3.1.3 or greater


.. py:function:: CDTInfinite()

    A type representing an infinte value. This type may only be used as a comparison value in operations.
    It may not be stored in the database.

    :return: a type representing an infinite value.

    .. code-block:: python

        import aerospike
        from aerospike_helpers.operations import list_operations as list_ops

        client = aerospike.client({'hosts': [('localhost', 3000)]}).connect()
        key = 'test', 'demo', 1

        #  get all values of the form [1, ...] from a list of lists.
        #  For example if list is [[1, 2, 3], [2, 3, 4], [1, 'a']], this operation will match
        #  [1, 2, 3] and [1, 'a']
        operations = [list_ops.list_get_by_value_range('list_bin', aerospike.LIST_RETURN_VALUE, [1],  [1, aerospike.CDTInfinite()])]
        _, _, bins = client.operate(key, operations)

    .. versionadded:: 3.5.0
    .. note:: This requires Aerospike Server 4.3.1.3 or greater


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


.. rubric:: Serialization

.. note::

    By default, the :class:`Client` maps the supported types \
    :py:class:`int`, :py:class:`str`, :py:class:`float`, :py:class:`bytearray`, \
    :py:class:`list`, :py:class:`dict` to matching aerospike server \
    `types <http://www.aerospike.com/docs/guide/data-types.html>`_ \
    (int, string, double, bytes, list, map). When an unsupported type is \
    encountered, the module uses \
    `cPickle <https://docs.python.org/2/library/pickle.html?highlight=cpickle#module-cPickle>`_ \
    to serialize and deserialize the data, storing it into *as_bytes* of type \
    `'Python' <https://www.aerospike.com/docs/udf/api/bytes.html#encoding-type>`_ \
    (`AS_BYTES_PYTHON <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).

    The functions :func:`~aerospike.set_serializer` and :func:`~aerospike.set_deserializer` \
    allow for user-defined functions to handle serialization, instead. \
    The serialized data is stored as \
    'Generic' *as_bytes* of type (\
    `AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_). \
    The *serialization* config param of :func:`aerospike.client` registers an \
    instance-level pair of functions that handle serialization.

.. py:function:: set_serializer(callback)

    Register a user-defined serializer available to all `Client`
    instances.

    :param callable callback: the function to invoke for serialization.

    .. seealso:: To use this function with :meth:`Client.put` the \
        argument to *serializer* should be :const:`aerospike.SERIALIZER_USER`.

    .. code-block:: python

        import aerospike
        import json

        def my_serializer(val):
            return json.dumps(val)

        aerospike.set_serializer(my_serializer)

    .. versionadded:: 1.0.39

.. py:function:: set_deserializer(callback)

    Register a user-defined deserializer available to all :class:`Client`
    instances. Once registered, all read methods (such as \
    :meth:`Client.get`) will run bins containing 'Generic' *as_bytes* \
    of type (`AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_)
    through this deserializer.

    :param callable callback: the function to invoke for deserialization.

.. py:function:: unset_serializers()

    Deregister the user-defined de/serializer available from :class:`Client`
    instances.

    .. versionadded:: 1.0.53

.. note:: Serialization Examples

    The following example shows the three modes of serialization - built-in, \
    class-level user functions, instance-level user functions:

    .. code-block:: python

        import aerospike
        import marshal
        import json

        def go_marshal(val):
            return marshal.dumps(val)

        def demarshal(val):
            return marshal.loads(val)

        def jsonize(val):
            return json.dumps(val)

        def dejsonize(val):
            return json.loads(val)

        aerospike.set_serializer(go_marshal)
        aerospike.set_deserializer(demarshal)
        config = {'hosts':[('127.0.0.1', 3000)]}
        client = aerospike.client(config).connect()
        config['serialization'] = (jsonize,dejsonize)
        client2 = aerospike.client(config).connect()

        for i in xrange(1, 4):
            try:
                client.remove(('test', 'demo', 'foo' + i))
            except:
                pass

        bin_ = {'t': (1, 2, 3)} # tuple is an unsupported type
        print("Use the built-in serialization (cPickle)")
        client.put(('test','demo','foo1'), bin_)
        (key, meta, bins) = client.get(('test','demo','foo1'))
        print(bins)

        print("Use the class-level user-defined serialization (marshal)")
        client.put(('test','demo','foo2'), bin_, serializer=aerospike.SERIALIZER_USER)
        (key, meta, bins) = client.get(('test','demo','foo2'))
        print(bins)

        print("Use the instance-level user-defined serialization (json)")
        client2.put(('test','demo','foo3'), bin_, serializer=aerospike.SERIALIZER_USER)
        # notice that json-encoding a tuple produces a list
        (key, meta, bins) = client2.get(('test','demo','foo3'))
        print(bins)
        client.close()

    The expected output is:

    .. code-block:: python

        Use the built-in serialization (cPickle)
        {'i': 321, 't': (1, 2, 3)}
        Use the class-level user-defined serialization (marshal)
        {'i': 321, 't': (1, 2, 3)}
        Use the instance-level user-defined serialization (json)
        {'i': 321, 't': [1, 2, 3]}

    While AQL shows the records as having the following structure:

    .. code-block:: sql

        aql> select i,t from test.demo where PK='foo1'
        +-----+----------------------------------------------+
        | i   | t                                            |
        +-----+----------------------------------------------+
        | 321 | 28 49 31 0A 49 32 0A 49 33 0A 74 70 31 0A 2E |
        +-----+----------------------------------------------+
        1 row in set (0.000 secs)

        aql> select i,t from test.demo where PK='foo2'
        +-----+-------------------------------------------------------------+
        | i   | t                                                           |
        +-----+-------------------------------------------------------------+
        | 321 | 28 03 00 00 00 69 01 00 00 00 69 02 00 00 00 69 03 00 00 00 |
        +-----+-------------------------------------------------------------+
        1 row in set (0.000 secs)

        aql> select i,t from test.demo where PK='foo3'
        +-----+----------------------------+
        | i   | t                          |
        +-----+----------------------------+
        | 321 | 5B 31 2C 20 32 2C 20 33 5D |
        +-----+----------------------------+
        1 row in set (0.000 secs)


.. rubric:: Logging

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


.. rubric:: Geospatial

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

.. _aerospike_operators:

Operators
=========

Operators for the single-record, multi-operation transaction method :py:meth:`Client.operate`.

.. note::

    Starting version 3.4.0, it is highly recommended to use the :ref:`aerospike_operation_helpers.operations` \
    to create the arguments for :py:meth:`Client.operate` and :py:meth:`Client.operate_ordered`
    Old style operatiors are deprecated. The docs for old style operators were removed in client 6.0.0.

.. _aerospike_policies:

Policy Options
==============

.. _POLICY_COMMIT_LEVEL:

Commit Level Policy Options
---------------------------

Specifies the number of replicas required to be successfully committed before returning success in a write operation to provide the desired consistency guarantee.

.. data:: POLICY_COMMIT_LEVEL_ALL

    Return succcess only after successfully committing all replicas

.. data:: POLICY_COMMIT_LEVEL_MASTER

    Return succcess after successfully committing the master replica


.. _POLICY_READ_MODE_AP:

AP Read Mode Policy Options
---------------------------

Read policy for AP (availability) namespaces.

.. data:: POLICY_READ_MODE_AP_ONE

    Involve single node in the read operation.

.. data:: POLICY_READ_MODE_AP_ALL

    Involve all duplicates in the read operation.

.. versionadded:: 3.7.0

.. _POLICY_READ_MODE_SC:

SC Read Mode Policy Options
---------------------------

Read policy for SC (strong consistency) namespaces.

.. data:: POLICY_READ_MODE_SC_SESSION

    Ensures this client will only see an increasing sequence of record versions. Server only reads from master. This is the default.

.. data:: POLICY_READ_MODE_SC_LINEARIZE

    Ensures ALL clients will only see an increasing sequence of record versions. Server only reads from master.

.. data:: POLICY_READ_MODE_SC_ALLOW_REPLICA

    Server may read from master or any full (non-migrating) replica. Increasing sequence of record versions is not guaranteed.

.. data:: POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE

    Server may read from master or any full (non-migrating) replica or from unavailable partitions. Increasing sequence of record versions is not guaranteed.

.. versionadded:: 3.7.0

.. _POLICY_EXISTS: 

Existence Policy Options
------------------------

Specifies the behavior for writing the record depending whether or not it exists.

.. data:: POLICY_EXISTS_CREATE

    Create a record, ONLY if it doesn't exist

.. data:: POLICY_EXISTS_CREATE_OR_REPLACE

    Completely replace a record if it exists, otherwise create it

.. data:: POLICY_EXISTS_IGNORE

    Write the record, regardless of existence. (i.e. create or update)

.. data:: POLICY_EXISTS_REPLACE

    Completely replace a record, ONLY if it exists

.. data:: POLICY_EXISTS_UPDATE

    Update a record, ONLY if it exists

.. _POLICY_GEN:

Generation Policy Options
-------------------------

Specifies the behavior of record modifications with regard to the generation value.

.. data:: POLICY_GEN_IGNORE

    Write a record, regardless of generation

.. data:: POLICY_GEN_EQ

    Write a record, ONLY if generations are equal

.. data:: POLICY_GEN_GT

    Write a record, ONLY if local generation is greater-than remote generation


.. _POLICY_KEY:

Key Policy Options
------------------

Specifies the behavior for whether keys or digests should be sent to the cluster.

.. data:: POLICY_KEY_DIGEST

    Calculate the digest on the client-side and send it to the server

.. data:: POLICY_KEY_SEND

    Send the key in addition to the digest. This policy causes a write operation to store the key on the server

.. _POLICY_REPLICA:

Replica Options
---------------

Specifies which partition replica to read from.

.. data:: POLICY_REPLICA_SEQUENCE

    Always try node containing master partition first. If connection fails and `retry_on_timeout` is true, try node containing prole partition. Currently restricted to master and one prole.

.. data:: POLICY_REPLICA_MASTER

    Read from the partition master replica node

.. data:: POLICY_REPLICA_ANY

    Distribute reads across nodes containing key's master and replicated partition in round-robin fashion. Currently restricted to master and one prole.

.. data:: POLICY_REPLICA_PREFER_RACK

    Try node on the same rack as the client first.  If there are no nodes on the same rack, use POLICY_REPLICA_SEQUENCE instead.

    **rack_aware** and **rack_id** must be set in the config argument of the client constructor in order to enable this functionality


Retry Policy Options
--------------------

Specifies the behavior of failed operations.

.. data:: POLICY_RETRY_NONE

    Only attempt an operation once

.. data:: POLICY_RETRY_ONCE

    If an operation fails, attempt the operation one more time


Constants
=========

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

    Use internal authentication only.  Hashed password is stored on the server. Do not send clear password. This is the default.

.. data:: AUTH_EXTERNAL

    Use external authentication (like LDAP).  Specific external authentication is configured on server.  If TLS defined, send clear password on node login via TLS. Throw exception if TLS is not defined.

.. data:: AUTH_EXTERNAL_INSECURE

    Use external authentication (like LDAP).  Specific external authentication is configured on server.  Send clear password on node login whether or not TLS is defined. This mode should only be used for testing purposes because it is not secure authentication.

.. _aerospike_scan_constants:

Scan Constants
--------------

.. data:: SCAN_PRIORITY

    .. deprecated:: 3.10.0
        Scan priority has been replaced by the records_per_second policy see :ref:`aerospike_scan_policies`.
        Scan priority will be removed in a coming release.

.. data:: SCAN_STATUS_ABORTED

    .. deprecated:: 1.0.50
        used by :meth:`Client.scan_info`

.. data:: SCAN_STATUS_COMPLETED

    .. deprecated:: 1.0.50
        used by :meth:`Client.scan_info`

.. data:: SCAN_STATUS_INPROGRESS

    .. deprecated:: 1.0.50
        used by :meth:`Client.scan_info`

.. data:: SCAN_STATUS_UNDEF

    .. deprecated:: 1.0.50
        used by :meth:`Client.scan_info`

.. versionadded:: 1.0.39

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

.. data:: SERIALIZER_PYTHON

    Use the cPickle serializer to handle unsupported types (default)

.. data:: SERIALIZER_USER

    Use a user-defined serializer to handle unsupported types. Must have \
    been registered for the aerospike class or configured for the Client object

.. data:: SERIALIZER_NONE

    Do not serialize bins whose data type is unsupported

.. versionadded:: 1.0.47

.. _send_bool_as_constants:

Send Bool Constants
-------------------

Specifies how the Python client will write Python booleans.

.. data:: PY_BYTES
    
    Write Python Booleans as PY_BYTES_BLOBs.
    
.. data:: INTEGER
    
    Write Python Booleans as integers.
    
.. data:: AS_BOOL
    
    Write Python Booleans as as_bools.

.. _aerospike_list_write_flag:

List Write Flags
--------------------
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
------------------

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
-----------------
Flags used by list order.

.. data:: LIST_UNORDERED

    List is not ordered. This is the default.

.. data:: LIST_ORDERED

    Ordered list.

.. _aerospike_list_sort_flag:

List Sort Flags
-----------------
Flags used by list sort.

.. data:: aerospike.LIST_SORT_DEFAULT

    Default. Preserve duplicates when sorting the list.

.. data:: aerospike.LIST_SORT_DROP_DUPLICATES

    Drop duplicate values when sorting the list.

.. _aerospike_map_write_flag:

Map Write Flag
-----------------
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
--------------

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
-----------------
Flags used by map order.

.. data:: MAP_UNORDERED

    Map is not ordered. This is the default.

.. data:: MAP_KEY_ORDERED

    Order map by key.

.. data:: MAP_KEY_VALUE_ORDERED

    Order map by key, then value.

.. _map_return_types:

Map Return Types
----------------

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

    Return key/value items. Note that key/value pairs will be returned as a list of tuples (i.e. [(key1, value1), (key2, value2)])


.. _aerospike_bitwise_write_flag:

Bitwise Write Flags
-----------------------

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
----------------------

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
----------------------

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
    
.. seealso:: `Data Types <http://www.aerospike.com/docs/guide/data-types.html>`_.

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


.. _regex_constants:

Regex Flag Values
------------------
Flags used for the `predexp.string_regex` function.

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

