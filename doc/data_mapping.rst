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


    The following table shows which Python types map directly to Aerospike server types.

+--------------------------+--------------+
| Python Type              | server type  |
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
|bytearray                 |bytes         |
+--------------------------+--------------+
|aerospike.GeoJSON         |GeoJSON       |
+--------------------------+--------------+

.. note::

	All other types will be stored as Python specific bytes. Python specific bytes may not be readable by Aerospike Clients for other languages.

