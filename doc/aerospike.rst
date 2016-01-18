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
    `System Overview <http://www.aerospike.com/docs/architecture/index.html>`_
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

            * **hosts** a required :class:`list` of (address, port) tuples identifying a node (or multiple nodes) in the cluster. \
              The client will connect to the first available node in the list, the *seed node*, \
              and will learn about the cluster and partition map from it.
            * **lua** an optional :class:`dict` containing the paths to two types of Lua modules
                * **system_path** the location of the system modules such as ``aerospike.lua`` (default: ``/usr/local/aerospike/lua``)
                * **user_path** the location of the user's record and stream UDFs
            * **serialization** an optional instance-level :py:func:`tuple` of (serializer, deserializer). Takes precedence over a class serializer registered with :func:`~aerospike.set_serializer`.
            * **policies** a :class:`dict` of policies
                * **timeout** default timeout in milliseconds
                * **key** default key policy for this client
                * **exists** default exists policy for this client
                * **gen** default generation policy for this client
                * **retry** default retry policy for this client
                * **consistency_level** default consistency level policy for this client
                * **replica** default replica policy for this client
                * **commit_level** default commit level policy for this client
            * **shm** a :class:`dict` with optional shared-memory cluster tending parameters. Shared-memory cluster tending is on if the :class:`dict` is provided. If multiple clients are instantiated talking to the same cluster the *shm* cluster-tending should be used.
                * **max_nodes** maximum number of nodes allowed. Pad so new nodes can be added without configuration changes (default: 16)
                * **max_namespaces** similarly pad (default: 8)
                * **takeover_threshold_sec** take over tending if the cluster hasn't been checked for this many seconds (default: 30)
                * **shm_key** explicitly set the shm key for this client. It is otherwise implicitly evaluated per unique hostname, and can be inspected with :meth:`~aerospike.Client.shm_key` (default: 0xA5000000)
            * **thread_pool_size** number of threads in the pool that is used in batch/scan/query commands (default: 16)
            * **max_threads** size of the synchronous connection pool for each server node (default: 300)
            * **batch_direct** whether to use the batch-direct protocol (default: ``False``, so will use batch-index if available)
            * **compression_threshold** compress data for transmission if the object size is greater than a given number of bytes (default: 0, or 'never compress')

    :return: an instance of the :py:class:`aerospike.Client` class.

    .. seealso::
        `Client Policies <http://www.aerospike.com/apidocs/c/db/d65/group__client__policies.html>`_ and \
        `Shared Memory <https://www.aerospike.com/docs/client/c/usage/shm.html>`_.

    .. code-block:: python

        import aerospike

        # configure the client to first connect to a cluster node at 127.0.0.1
        # the client will learn about the other nodes in the cluster from the
        # seed node.
        # in this configuration shared-memory cluster tending is turned on,
        # which is appropriate for a multi-process context, such as a webserver
        config = {
            'hosts':    [ ('127.0.0.1', 3000) ],
            'policies': {'timeout': 1000},
            'shm':      { }}
        client = aerospike.client(config)

    .. versionchanged:: 1.0.56


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

.. data:: OPERATOR_APPEND

    The append-to-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_INCR

    The increment-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_PREPEND

    The prepend-to-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_READ

    The read-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_TOUCH

    The touch-record operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. data:: OPERATOR_WRITE

    The write-bin operator for the multi-ops method :py:meth:`~aerospike.Client.operate`

.. _aerospike_policies:

Policies
--------

.. data:: POLICY_COMMIT_LEVEL_ALL

    An option of the *'commit_level'* policy

.. data:: POLICY_COMMIT_LEVEL_MASTER

.. data:: POLICY_CONSISTENCY_ALL

    An option of the *'consistency_level'* policy

.. data:: POLICY_CONSISTENCY_ONE

.. data:: POLICY_EXISTS_CREATE

    An option of the *'exists'* policy

.. data:: POLICY_EXISTS_CREATE_OR_REPLACE

.. data:: POLICY_EXISTS_IGNORE

.. data:: POLICY_EXISTS_REPLACE

.. data:: POLICY_EXISTS_UPDATE

.. data:: POLICY_GEN_EQ

    An option of the *'gen'* policy

.. data:: POLICY_GEN_GT

.. data:: POLICY_GEN_IGNORE

.. data:: POLICY_KEY_DIGEST

    An option of the *'key'* policy

.. data:: POLICY_KEY_SEND

.. data:: POLICY_REPLICA_ANY

    An option of the *'replica'* policy

.. data:: POLICY_REPLICA_MASTER

.. data:: POLICY_RETRY_NONE

    An option of the *'retry'* policy

.. data:: POLICY_RETRY_ONCE

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

    .. versionadded:: 1.0.57

.. data:: UDF_TYPE_LUA

.. data:: INDEX_STRING

    An index whose values are of the aerospike string data type

.. data:: INDEX_NUMERIC

    An index whose values are of the aerospike integer data type

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

.. data:: PRIV_READ

    The user is granted read access.

.. data:: PRIV_READ_WRITE

    The user is granted read and write access.

.. data:: PRIV_READ_WRITE_UDF

    The user is granted read and write access, and the ability to invoke UDFs.

.. data:: PRIV_SYS_ADMIN

    The user is granted the ability to perform system administration operations.

.. data:: PRIV_USER_ADMIN

    The user is granted the ability to perform user administration operations.


