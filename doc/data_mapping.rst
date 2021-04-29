.. _Data_Mapping:

*************************************************
:mod:`Data_Mapping` --- Python Data Mappings
*************************************************

.. rubric:: How Python types map to server types

.. note::

    By default, the :py:class:`aerospike.Client` maps the supported types \
    :py:class:`int`, :py:class:`bool`, :py:class:`str`, :py:class:`float`, :py:class:`bytearray`, \
    :py:class:`list`, :py:class:`dict` to matching aerospike server \
    `types <http://www.aerospike.com/docs/guide/data-types.html>`_ \
    (int, string, double, blob, list, map). When an unsupported type is \
    encountered, the module uses \
    `cPickle <https://docs.python.org/2/library/pickle.html?highlight=cpickle#module-cPickle>`_ \
    to serialize and deserialize the data, storing it into a blob of type \
    `'Python' <https://www.aerospike.com/docs/udf/api/bytes.html#encoding-type>`_ \
    (`AS_BYTES_PYTHON <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).

    The functions :func:`~aerospike.set_serializer` and :func:`~aerospike.set_deserializer` \
    allow for user-defined functions to handle serialization, instead. The user provided function will be run instead of cPickle. \
    The serialized data is stored as \
    type (\
    `AS_BYTES_BLOB <http://www.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_). \
    This type allows the storage of binary data readable by Aerospike Clients in other languages. \
    The *serialization* config param of :func:`aerospike.client` registers an \
    instance-level pair of functions that handle serialization.

    Unless a user specified serializer has been provided, all other types will be stored as Python specific bytes. Python specific bytes may not be readable by Aerospike Clients for other languages.

.. warning::

    Aerospike is introducing a new boolean data type in server version 5.6.
    Python client version 6.x is a jump version that reads the new bool type as an integer.
    Version 6.x also writes Python booleans as integers to the database instead of serializing them as AS_BYTES_PYTHON.
    This was done to provide a jump version for users to perform a rolling upgrade to the next major version of the Python client that will offer full boolean support.
    **This means that applications that write or read native python booleans to the databse could break depending on how truth value is tested**
    When the Python client fully supports the new server boolean data type, it will read them as native Python booleans but write Python booleans as server booleans.
    All clients will support the new server booleans so this will increase cross client compatibility.

    See the example below for version behavior.
    .. example::

        import aerospike

        # Configure the client.
        config = {
            "hosts": [("127.0.0.1", 3000)]
        }

        # Create a client and connect it to the cluster.
        client = aerospike.client(config).connect()

        TEST_NS = "test"
        TEST_SET = "demo"
        RECORD = {"bool_bin": True}
        KEY = (TEST_NS, TEST_SET, "bool")

        # Write the bool.
        client.put(KEY, RECORD)

        # Read the bool.
        _, _, res = client.get(KEY)
        print(res)

        # Close the connection to the Aerospike cluster.
        client.remove(KEY)
        client.close()

        # EXPECTED OUTPUT PRE 6.0.0:
        # Python bool is stored in aerospike as PY_BYTES_BLOB and read by client as Python boolean.
        # {'bool_bin': True}

        # EXPECTED OUTPUT 6.0.0:
        # Python bool is stored in aerospike as int and read by client as int.
        # {'bool_bin': 1}

        # EXPECTED OUTPUT POST 6.0.0:
        # Python bool is stored in aerospike as new bool type and read by client as Python boolean.
        # {'bool_bin': True}

    Depending on how your application treats Python booleans that are read from the server, your application could break when using client version 6.0.0.
    For example, testing the read value, x, as `x is True` will fail in client version 6.0.0. Testing for truth value with `x == True` will work in all versions.
    See the modified check from the previous example.
    .. example::

        # Read the bool.
        _, _, res = client.get(KEY)
        bool_bin_val = res["bool_bin"]
        print(bool_bin_val is True)

        # EXPECTED OUTPUT PRE 6.0.0:
        # Python bool is stored in aerospike as PY_BYTES_BLOB and read by client as Python boolean.
        # True

        # EXPECTED OUTPUT 6.0.0:
        # Python bool is stored in aerospike as int and read by client as int.
        # False

        # EXPECTED OUTPUT POST 6.0.0:
        # Python bool is stored in aerospike as new bool type and read by client as Python boolean.
        # True
    
    If only truth value is checked, then all versions should work. See below.
    .. example::

        # Read the bool.
        _, _, res = client.get(KEY)
        bool_bin_val = res["bool_bin"]
        print(bool(bool_bin_val))

        # EXPECTED OUTPUT PRE 6.0.0:
        # Python bool is stored in aerospike as PY_BYTES_BLOB and read by client as Python boolean.
        # True

        # EXPECTED OUTPUT 6.0.0:
        # Python bool is stored in aerospike as int and read by client as int.
        # True

        # EXPECTED OUTPUT POST 6.0.0:
        # Python bool is stored in aerospike as new bool type and read by client as Python boolean.
        # True

The following table shows which Python types map directly to Aerospike server types.

.. note::

    :class:`aerospike.KeyOrderedDict` is a special case. Like dict, KeyOrderedDict maps to the aerospike map data type. However, the map will be sorted in key order before being sent to the server, see :ref:`aerospike_map_order`.

+--------------------------+---------------+
| Python Type              | Server type   |
+==========================+===============+
|int                       |integer        |
+--------------------------+---------------+
|bool                      |integer        |
+--------------------------+---------------+
|str                       |string         |
+--------------------------+---------------+
|unicode                   |string         |
+--------------------------+---------------+
|float                     |double         |
+--------------------------+---------------+
|dict                      |map            |
+--------------------------+---------------+
|aerospike.KeyOrderedDict  |key ordered map|
+--------------------------+---------------+
|list                      |list           |
+--------------------------+---------------+
|bytearray                 |blob           |
+--------------------------+---------------+
|aerospike.GeoJSON         |GeoJSON        |
+--------------------------+---------------+

It is possible to nest these datatypes. For example a list may contain a dictionary, or a dictionary may contain a list as a value.

.. note::

	Unless a user specified serializer has been provided, all other types will be stored as Python specific bytes. Python specific bytes may not be readable by Aerospike Clients for other languages.

