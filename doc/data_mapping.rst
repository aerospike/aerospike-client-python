.. _Data_Mapping:

*************************************************
:mod:`Data_Mapping` --- Python Data Mappings
*************************************************

.. rubric:: How Python types map to server types

.. note::

    By default, the :py:class:`aerospike.Client` maps the supported types \
    :py:class:`int`, :py:class:`str`, :py:class:`float`, :py:class:`bytearray`, \
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

The following table shows which Python types map directly to Aerospike server types.

+--------------------------+--------------+
| Python Type              | Server type  |
+==========================+==============+
|int                       |integer       |
+--------------------------+--------------+
|long                      |integer       |
+--------------------------+--------------+
|str                       |string        |
+--------------------------+--------------+
|unicode                   |string        |
+--------------------------+--------------+
|float                     |double        |
+--------------------------+--------------+
|dict                      |map           |
+--------------------------+--------------+
|list                      |list          |
+--------------------------+--------------+
|bytearray                 |blob          |
+--------------------------+--------------+
|aerospike.GeoJSON         |GeoJSON       |
+--------------------------+--------------+

It is possible to nest these datatypes. For example a list may contain a dictionary, or a dictionary may contain a list as a value.

.. note::

	Unless a user specified serializer has been provided, all other types will be stored as Python specific bytes. Python specific bytes may not be readable by Aerospike Clients for other languages.

