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


.. py:function:: client(config)

    Creates a new instance of the Client class. This client can
    :meth:`~aerospike.Client.connect` to the cluster and perform operations
    against it, such as :meth:`~aerospike.Client.put` and
    :meth:`~aerospike.Client.get` records.

    :param dict config: the client's configuration.

        .. hlist::
            :columns: 1

            * **hosts** a required :class:`list` of (address, port, [tls-name]) tuples identifying a node (or multiple nodes) in the cluster. \
              The client will connect to the first available node in the list, the *seed node*, \
              and will learn about the cluster and partition map from it. If tls-name is specified, it must match the tls-name specified in the node's
              server configuration file and match the server's CA certificate. **Note: use of TLS requires Aerospike Enterprise Edition**
            * **lua** an optional :class:`dict` containing the paths to two types of Lua modules
                * **system_path** the location of the system modules such as ``aerospike.lua`` (default: ``/usr/local/aerospike/lua``)
                * **user_path** the location of the user's record and stream UDFs . (default: ``./``)
            * **policies** a :class:`dict` of policies
                * **read** A dictionary containing read policies. See :ref:`aerospike_read_policies` for available policy fields and values.
                * **write** A dictionary containing write policies. See :ref:`aerospike_write_policies` for available policy fields and values.
                * **apply** A dictionary containing apply policies. See :ref:`aerospike_apply_policies` for available policy fields and values.
                * **operate** A dictionary containing operate policies. See :ref:`aerospike_operate_policies` for available policy fields and values.
                * **remove** A dictionary containing remove policies. See :ref:`aerospike_remove_policies` for available policy fields and values.
                * **query** A dictionary containing query policies. See :ref:`aerospike_query_policies` for available policy fields and values.
                * **scan** A dictionary containing scan policies. See :ref:`aerospike_scan_policies` for available policy fields and values.
                * **batch** A dictionary containing batch policies. See :ref:`aerospike_batch_policies` for available policy fields and values.
                * **total_timeout** default connection timeout in milliseconds (**Deprecated**: set this the individual policy dictionaries)
                * **key** default key policy, with values such as :data:`aerospike.POLICY_KEY_DIGEST` (**Deprecated**: set this individually in the 'read', 'write', 'apply', 'operate', 'remove' policy dictionaries)
                * **exists** default exists policy, with values such as :data:`aerospike.POLICY_EXISTS_CREATE` (**Deprecated**: set in the 'write' policies dictionary)
                * **max_retries** a :class:`int` representing the number of times to retry a transaction (**Deprecated**: set this the individual policy dictionaries)
                * **consistency_level** default consistency level policy, with values such as :data:`aerospike.POLICY_CONSISTENCY_ONE` (**Deprecated**: set this individually as needed in the 'read','operate', 'batch' policy dictionaries)
                * **replica** default replica policy, with values such as :data:`aerospike.POLICY_REPLICA_MASTER` (**Deprecated**: set this in one or all of the 'read', 'write', 'apply', 'operate', 'remove' policy dictionaries)
                * **commit_level** default commit level policy, with values such as :data:`aerospike.POLICY_COMMIT_LEVEL_ALL` (**Deprecated**: set this as needed individually in the 'write', 'apply', 'operate', 'remove' policy dictionaries)
            * **shm** a :class:`dict` with optional shared-memory cluster tending parameters. Shared-memory cluster tending is on if the :class:`dict` is provided. If multiple clients are instantiated talking to the same cluster the *shm* cluster-tending should be used.
                * **max_nodes** maximum number of nodes allowed. Pad so new nodes can be added without configuration changes (default: 16)
                * **max_namespaces** similarly pad (default: 8)
                * **takeover_threshold_sec** take over tending if the cluster hasn't been checked for this many seconds (default: 30)
                * **shm_key** explicitly set the shm key for this client. If **use_shared_connection** is not set, or set to `False`, the user must provide a value for this field in order for shared memory to work correctly. If , and only if, **use_shared_connection** is set to `True`, the key will be implicitly evaluated per unique hostname, and can be inspected with :meth:`~aerospike.Client.shm_key` . It is still possible to specify a key when using **use_shared_connection** = `True`. (default: 0xA7000000)
            * **use_shared_connection** :class:`bool` indicating whether this instance should share its connection to the Aerospike cluster with other client instances in the same process. (default: ``False``)
            * **tls** a :class:`dict` of optional TLS configuration parameters. **TLS usage requires Aerospike Enterprise Edition**
                * **enable** a :class:`bool` indicating whether tls should be enabled or not. Default: ``False``
                * **cafile** :class:`str` Path to a trusted CA certificate file. By default TLS will use system standard trusted CA certificates
                * **capath** :class:`str` Path to a directory of trusted certificates. See the OpenSSL SSL_CTX_load_verify_locations manual page for more information about the format of the directory.
                * **protocols** Specifies enabled protocols. This format is the same as Apache's SSLProtocol documented at https://httpd.apache.org/docs/current/mod/mod_ssl.html#sslprotocol . If not specified the client will use "-all +TLSv1.2".
                * **cipher_suite** :class:`str` Specifies enabled cipher suites. The format is the same as OpenSSL's Cipher List Format documented at https://www.openssl.org/docs/manmaster/apps/ciphers.html .If not specified the OpenSSL default cipher suite described in the ciphers documentation will be used. If you are not sure what cipher suite to select this option is best left unspecified 
                * **keyfile** :class:`str` Path to the client's key for mutual authentication. By default mutual authentication is disabled.
                * **cert_blacklist** :class:`str` Path to a certificate blacklist file. The file should contain one line for each blacklisted certificate. Each line starts with the certificate serial number expressed in hex. Each entry may optionally specify the issuer name of the certificate (serial numbers are only required to be unique per issuer). Example records: 867EC87482B2 /C=US/ST=CA/O=Acme/OU=Engineering/CN=Test Chain CA E2D4B0E570F9EF8E885C065899886461
                * **certfile** :class:`str` Path to the client's certificate chain file for mutual authentication. By default mutual authentication is disabled.
                * **crl_check** :class:`bool` Enable CRL checking for the certificate chain leaf certificate. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
                * **crl_check_all** :class:`bool` Enable CRL checking for the entire certificate chain. An error occurs if a suitable CRL cannot be found. By default CRL checking is disabled.
                * **log_session_info** :class:`bool` Log session information for each connection.
            * **serialization** an optional instance-level :py:func:`tuple` of (serializer, deserializer). Takes precedence over a class serializer registered with :func:`~aerospike.set_serializer`.
            * **thread_pool_size** number of threads in the pool that is used in batch/scan/query commands (default: 16)
            * **max_socket_idle** Maximum socket idle time in seconds.  Connection pools will discard sockets that have
			been idle longer than the maximum.  The value is limited to 24 hours (86400).
	 		It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
			(default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
			that has already been reaped by the server.
			Default: 0 seconds (disabled) for non-TLS connections, 55 seconds for TLS connections.
            * **max_conns_per_node** maximum number of pipeline connections allowed for each node 
            * **batch_direct** whether to use the batch-direct protocol (default: ``False``, so will use batch-index if available) (**Deprecated**: set 'use_batch_direct' in the batch policy dictionary)
            * **tend_interval** polling interval in milliseconds for tending the cluster (default: 1000)
            * **compression_threshold** compress data for transmission if the object size is greater than a given number of bytes (default: 0, meaning 'never compress') (**Deprecated**, set this in the 'write' policy dictionary)
            * **cluster_name** only server nodes matching this name will be used when determining the cluster

    :return: an instance of the :py:class:`aerospike.Client` class.

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

    By default, the :py:class:`aerospike.Client` maps the supported types \
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

    Register a user-defined serializer available to all :class:`aerospike.Client`
    instances.

    :param callable callback: the function to invoke for serialization.

    .. seealso:: To use this function with :meth:`~aerospike.Client.put` the \
        argument to *serializer* should be :const:`aerospike.SERIALIZER_USER`.

    .. code-block:: python

        import aerospike
        import json

        def my_serializer(val):
            return json.dumps(val)

        aerospike.set_serializer(my_serializer)

    .. versionadded:: 1.0.39

.. py:function:: set_deserializer(callback)

    Register a user-defined deserializer available to all :class:`aerospike.Client`
    instances. Once registered, all read methods (such as \
    :meth:`~aerospike.Client.get`) will run bins containing 'Generic' *as_bytes* \
    of type (`AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_)
    through this deserializer.

    :param callable callback: the function to invoke for deserialization.

.. py:function:: unset_serializers()

    Deregister the user-defined de/serializer available from :class:`aerospike.Client`
    instances.

    .. versionadded:: 1.0.53

.. note:: Serialization Examples

    The following example shows the three modes of serialization - built-in, \
    class-level user functions, instance-level user functions:

    .. code-block:: python

        from __future__ import print_function
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

    Set a user-defined function as the log handler for all aerospike objects.
    The *callback* is invoked whenever a log event passing the logging level
    threshold is encountered.

    :param callable callback: the function used as the logging handler.

    .. note:: The callback function must have the five parameters (level, func, path, line, msg)

        .. code-block:: python

            from __future__ import print_function
            import aerospike

            def as_logger(level, func, path, line, msg):
            def as_logger(level, func, myfile, line, msg):
                print("**", myfile, line, func, ':: ', msg, "**")

            aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
            aerospike.set_log_handler(as_logger)


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
---------

Operators for the multi-ops method :py:meth:`~aerospike.Client.operate`.

.. data:: OPERATOR_WRITE

    Write a value into a bin

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_WRITE,
            "bin": "name",
            "val": "Peanut"
        }

.. data:: OPERATOR_APPEND

    Append to a bin with :class:`str` type data

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_APPEND,
            "bin": "name",
            "val": "Mr. "
        }

.. data:: OPERATOR_PREPEND

    Prepend to a bin with :class:`str` type data

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_PREPEND,
            "bin": "name",
            "val": " Esq."
        }

.. data:: OPERATOR_INCR

    Increment a bin with :class:`int` or :class:`float` type data

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_INCR,
            "bin": "age",
            "val": 1
        }

.. data:: OPERATOR_READ

    Read a specific bin

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_READ,
            "bin": "name"
        }

.. data:: OPERATOR_TOUCH

    Touch a record, setting its TTL. May be combined with :const:`~aerospike.OPERATOR_READ`

    .. code-block:: python

        {
            "op" : aerospike.OPERATOR_TOUCH
        }

.. data:: OP_LIST_APPEND

    Append an element to a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_APPEND,
            "bin": "events",
            "val": 1234
        }

.. data:: OP_LIST_APPEND_ITEMS

    Extend a bin with :class:`list` type data with a list of items

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_APPEND_ITEMS,
            "bin": "events",
            "val": [ 123, 456 ]
        }

.. data:: OP_LIST_INSERT

    Insert an element at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_INSERT,
            "bin": "events",
            "index": 2,
            "val": 1234
        }

.. data:: OP_LIST_INSERT_ITEMS

    Insert the items at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_INSERT_ITEMS,
            "bin": "events",
            "index": 2,
            "val": [ 123, 456 ]
        }

.. data:: OP_LIST_INCREMENT

    Increment the value of an item at the given index in a list stored in the specified bin

    .. code-block:: python

        {
            "op": aerospike.OP_LIST_INCREMENT,
            "bin": "bin_name",
            "index": 2,
            "val": 5
        }

.. data:: OP_LIST_POP

    Remove and return the element at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_POP, # removes and returns a value
            "bin": "events",
            "index": 2
        }

.. data:: OP_LIST_POP_RANGE

    Remove and return a list of elements at a specified index range of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_POP_RANGE,
            "bin": "events",
            "index": 2,
            "val": 3 # remove and return 3 elements starting at index 2
        }

.. data:: OP_LIST_REMOVE

    Remove the element at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_REMOVE, # remove a value
            "bin": "events",
            "index": 2
        }

.. data:: OP_LIST_REMOVE_RANGE

    Remove a list of elements at a specified index range of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_REMOVE_RANGE,
            "bin": "events",
            "index": 2,
            "val": 3 # remove 3 elements starting at index 2
        }

.. data:: OP_LIST_CLEAR

    Remove all the elements in a bin with :class:`list` type data

    .. code-block:: python

         {
            "op" : aerospike.OP_LIST_CLEAR,
            "bin": "events"
        }

.. data:: OP_LIST_SET

    Set the element *val* in a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_SET,
            "bin": "events",
            "index": 2,
            "val": "latest event at index 2" # set this value at index 2
        }

.. data:: OP_LIST_GET

    Get the element at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_GET,
            "bin": "events",
            "index": 2
        }

.. data:: OP_LIST_GET_RANGE

    Get the list of elements starting at a specified index of a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_GET_RANGE,
            "bin": "events",
            "index": 2,
            "val": 3 # get 3 elements starting at index 2
        }

.. data:: OP_LIST_TRIM

    Remove elements from a bin with :class:`list` type data which are not within the range starting at a given *index* plus *val*

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_TRIM,
            "bin": "events",
            "index": 2,
            "val": 3 # remove all elements not in the range between index 2 and index 2 + 3
        }

.. data:: OP_LIST_SIZE

    Count the number of elements in a bin with :class:`list` type data

    .. code-block:: python

        {
            "op" : aerospike.OP_LIST_SIZE,
            "bin": "events" # gets the size of a list contained in the bin
        }

.. data:: OP_MAP_SET_POLICY

    Set the policy for a map bin. The policy controls the write mode and the ordering of the map entries.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_SET_POLICY,
            "bin": "scores",
            "map_policy": {"map_write_mode": Aeorspike.MAP_UPDATE, "map_order": Aerospike.MAP_KEY_VALUE_ORDERED}
        }

.. data:: OP_MAP_PUT

    Put a key/value pair into a map. Operator accepts an optional map_policy dictionary (see OP_MAP_SET_POLICY for an example)

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_PUT,
            "bin": "my_map",
            "key": "age",
            "val": 97
        }

.. data:: OP_MAP_PUT_ITEMS. Operator accepts an optional map_policy dictionary (see OP_MAP_SET_POLICY for an example)

    Put a dictionary of key/value pairs into a map.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_PUT_ITEMS,
            "bin": "my_map",
            "val": {"name": "bubba", "occupation": "dancer"}
        }

.. data:: OP_MAP_INCREMENT. Operator accepts an optional map_policy dictionary (see OP_MAP_SET_POLICY for an example)

    Increment the value of map entry by the given "val" argument.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_INCREMENT,
            "bin": "my_map",
            "key": "age",
            "val": 1
        }

.. data:: OP_MAP_DECREMENT. Operator accepts an optional map_policy dictionary (see OP_MAP_SET_POLICY for an example)

    Decrement the value of map entry by the given "val" argument.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_DECREMENT,
            "bin": "my_map",
            "key": "age",
            "val": 1
        }

.. data:: OP_MAP_SIZE

    Return the number of entries in the given map bin.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_SIZE,
            "bin": "my_map"
        }

.. data:: OP_MAP_CLEAR

    Remove all entries from the given map bin.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_CLEAR,
            "bin": "my_map"
        }

Note that if "return_type" is not specified in the parameters for a map operation, the default is aerospike.MAP_RETURN_NONE

.. data:: OP_MAP_REMOVE_BY_KEY

    Remove the first entry from the map bin that matches the given key.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_KEY,
            "bin": "my_map",
            "key": "age",
            "return_type": aerospike.MAP_RETURN_VALUE
        }

.. data:: OP_MAP_REMOVE_BY_KEY_LIST

    Remove the entries from the map bin that match the list of given keys.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_KEY_LIST,
            "bin": "my_map",
            "val": ["name", "rank", "serial"]
        }

.. data:: OP_MAP_REMOVE_BY_KEY_RANGE

    Remove the entries from the map bin that have keys which fall between the given "key" (inclusive) and "val" (exclusive).

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_KEY_RANGE,
            "bin": "my_map",
            "key": "i",
            "val": "j",
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_REMOVE_BY_VALUE

    Remove the entry or entries from the map bin that have values which match the given "val" parameter.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_VALUE,
            "bin": "my_map",
            "val": 97,
            "return_type": aerospike.MAP_RETURN_KEY
        }

.. data:: OP_MAP_REMOVE_BY_VALUE_LIST

    Remove the entries from the map bin that have values which match the list of values given in the "val" parameter.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_VALUE_LIST,
            "bin": "my_map",
            "val": [97, 98, 99],
            "return_type": aerospike.MAP_RETURN_KEY
        }

.. data:: OP_MAP_REMOVE_BY_VALUE_RANGE

    Remove the entries from the map bin that have values starting with the given "val" parameter (inclusive) up to the given "range" parameter (exclusive).

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE,
            "bin": "my_map",
            "val": 97,
            "range": 100,
            "return_type": aerospike.MAP_RETURN_KEY
        }

.. data:: OP_MAP_REMOVE_BY_INDEX

    Remove the entry from the map bin at the given "index" location.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_INDEX,
            "bin": "my_map",
            "index": 0,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_REMOVE_BY_INDEX_RANGE

    Remove the entries from the map bin starting at the given "index" location and removing "range" items.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE,
            "bin": "my_map",
            "index": 0,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }
        
.. data:: OP_MAP_REMOVE_BY_RANK

    Remove the first entry from the map bin that has a value with a rank matching the given "index".

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_RANK,
            "bin": "my_map",
            "index": 10
        }

.. data:: OP_MAP_REMOVE_BY_RANK_RANGE

    Remove the entries from the map bin that have values with a rank starting at the given "index" and removing "range" items.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_REMOVE_BY_RANK_RANGE,
            "bin": "my_map",
            "index": 10,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_GET_BY_KEY

    Return the entry from the map bin that which has a key that matches the given "key" parameter.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_KEY,
            "bin": "my_map",
            "key": "age",
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_GET_BY_KEY_RANGE

    Return the entries from the map bin that have keys which fall between the given "key" (inclusive) and "val" (exclusive).

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_KEY_RANGE,
            "bin": "my_map",
            "key": "i",
            "range": "j",
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_GET_BY_VALUE

    Return the entry or entries from the map bin that have values which match the given "val" parameter.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_VALUE,
            "bin": "my_map",
            "val": 97,
            "return_type": aerospike.MAP_RETURN_KEY
        }

.. data:: OP_MAP_GET_BY_VALUE_RANGE

    Return the entries from the map bin that have values starting with the given "val" parameter (inclusive) up to the given "range" parameter (exclusive).

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_VALUE_RANGE,
            "bin": "my_map",
            "val": 97,
            "range": 100,
            "return_type": aerospike.MAP_RETURN_KEY
        }

.. data:: OP_MAP_GET_BY_INDEX

    Return the entry from the map bin at the given "index" location.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_INDEX,
            "bin": "my_map",
            "index": 0,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_GET_BY_INDEX_RANGE

    Return the entries from the map bin starting at the given "index" location and removing "range" items.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_INDEX_RANGE,
            "bin": "my_map",
            "index": 0,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. data:: OP_MAP_GET_BY_RANK

    Return the first entry from the map bin that has a value with a rank matching the given "index".

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_RANK,
            "bin": "my_map",
            "index": 10
        }

.. data:: OP_MAP_GET_BY_RANK_RANGE

    Return the entries from the map bin that have values with a rank starting at the given "index" and removing "range" items.

    .. code-block:: python

        {
            "op" : aerospike.OP_MAP_GET_BY_RANK_RANGE,
            "bin": "my_map",
            "index": 10,
            "val": 2,
            "return_type": aerospike.MAP_RETURN_KEY_VALUE
        }

.. versionchanged:: 2.0.4

.. _aerospike_policies:

Policies
--------

.. rubric:: Commit Level Policy Options

Specifies the number of replicas required to be successfully committed before returning success in a write operation to provide the desired consistency guarantee.

.. data:: POLICY_COMMIT_LEVEL_ALL

    Return succcess only after successfully committing all replicas

.. data:: POLICY_COMMIT_LEVEL_MASTER

    Return succcess after successfully committing the master replica

.. rubric:: Consistency Level Policy Options

Specifies the number of replicas to be consulted in a read operation to provide the desired consistency guarantee.

.. data:: POLICY_CONSISTENCY_ONE

    Involve a single replica in the operation

.. data:: POLICY_CONSISTENCY_ALL

    Involve all replicas in the operation

.. rubric:: Existence Policy Options

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

.. rubric:: Generation Policy Options

Specifies the behavior of record modifications with regard to the generation value.

.. data:: POLICY_GEN_IGNORE

    Write a record, regardless of generation

.. data:: POLICY_GEN_EQ

    Write a record, ONLY if generations are equal

.. data:: POLICY_GEN_GT

    Write a record, ONLY if local generation is greater-than remote generation

.. rubric:: Key Policy Options

Specifies the behavior for whether keys or digests should be sent to the cluster.

.. data:: POLICY_KEY_DIGEST

    Calculate the digest on the client-side and send it to the server

.. data:: POLICY_KEY_SEND

    Send the key in addition to the digest. This policy causes a write operation to store the key on the server

.. rubric:: Replica Options

Specifies which partition replica to read from.

.. data:: POLICY_REPLICA_MASTER

    Read from the partition master replica node

.. data:: POLICY_REPLICA_ANY

    Distribute reads across nodes containing key's master and replicated partition in round-robin fashion. Currently restricted to master and one prole.

.. rubric:: Retry Policy Options

Specifies the behavior of failed operations.

.. data:: POLICY_RETRY_NONE

    Only attempt an operation once

.. data:: POLICY_RETRY_ONCE

    If an operation fails, attempt the operation one more time

.. _aerospike_scan_constants:

Scan Constants
--------------

.. data:: SCAN_PRIORITY_AUTO

.. data:: SCAN_PRIORITY_HIGH

.. data:: SCAN_PRIORITY_LOW

.. data:: SCAN_PRIORITY_MEDIUM

.. data:: SCAN_STATUS_ABORTED

    .. deprecated:: 1.0.50
        used by :meth:`~aerospike.Client.scan_info`

.. data:: SCAN_STATUS_COMPLETED

    .. deprecated:: 1.0.50
        used by :meth:`~aerospike.Client.scan_info`

.. data:: SCAN_STATUS_INPROGRESS

    .. deprecated:: 1.0.50
        used by :meth:`~aerospike.Client.scan_info`

.. data:: SCAN_STATUS_UNDEF

    .. deprecated:: 1.0.50
        used by :meth:`~aerospike.Client.scan_info`

.. versionadded:: 1.0.39

.. _aerospike_job_constants:

Job Constants
--------------

.. data:: JOB_SCAN

    Scan job type argument for the module parameter of :meth:`~aerospike.Client.job_info`

.. data:: JOB_QUERY

    Query job type argument for the module parameter of :meth:`~aerospike.Client.job_info`

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

.. _aerospike_misc_constants:

Miscellaneous
-------------

.. data:: __version__

    A :class:`str` containing the module's version.

    .. versionadded:: 1.0.54

.. data:: null

    A value for distinguishing a server-side null from a Python :py:obj:`None` .

    .. deprecated:: 2.0.1
        use the function :func:`aerospike.null` instead.

.. data:: UDF_TYPE_LUA

.. data:: INDEX_STRING

    An index whose values are of the aerospike string data type

.. data:: INDEX_NUMERIC

    An index whose values are of the aerospike integer data type

.. data:: INDEX_GEO2DSPHERE

    An index whose values are of the aerospike GetJSON data type
    
.. seealso:: `Data Types <http://www.aerospike.com/docs/guide/data-types.html>`_.

.. data:: INDEX_TYPE_LIST

    Index a bin whose contents is an aerospike list

.. data:: INDEX_TYPE_MAPKEYS

    Index the keys of a bin whose contents is an aerospike map

.. data:: INDEX_TYPE_MAPVALUES

    Index the values of a bin whose contents is an aerospike map

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


.. _map_return_types:

Map Return Types
----------------

Return types used by various map operations

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

