.. _Data_Mapping:

*************************************************
Python Data Mappings
*************************************************

.. rubric:: How Python types map to server types

Default Behavior
----------------

By default, the :py:class:`~aerospike.Client` maps the supported Python types to Aerospike server \
`types <https://aerospike.com/docs/develop/client/python/data-types/>`_. \
When an unsupported type is encountered by the module:

1. When sending data to the server, it does not serialize the type and will throw an error.
2. When reading `AS_BYTES_PYTHON` types from the server, it returns the raw bytes as a :class:`bytearray`.
   To deserialize this data, the application must use cPickle instead of relying on the client to do it automatically.

Serializers
-----------

However, the functions :func:`~aerospike.set_serializer` and :func:`~aerospike.set_deserializer` \
allow for user-defined functions to handle serialization.
The serialized data is stored in the server with generic encoding \
(`AS_BYTES_BLOB <https://docs.aerospike.com/apidocs/c/d0/dd4/as__bytes_8h.html#a0cf2a6a1f39668f606b19711b3a98bf3>`_).
This type allows the storage of binary data readable by Aerospike Clients in other languages. \
The *serialization* config parameter of :func:`aerospike.client` registers an \
instance-level pair of functions that handle serialization.

.. warning::

    *Aerospike is introducing a new boolean data type in server version 5.6.*

    In order to support cross client compatibility and rolling upgrades, Python client version ``6.x`` comes with a new client config, ``send_bool_as``.
    It configures how the client writes Python booleans and allows for opting into using the new boolean type.
    It is important to consider how other clients connected to the Aerospike database write booleans in order to maintain cross client compatibility.
    For example, if there is a client that reads and writes booleans as integers, then another Python client working with the same data should do the same thing.

    ``send_bool_as`` can be set so the client writes Python booleans as integers or the Aerospike native boolean type.

    All versions before ``6.x`` wrote Python booleans as ``AS_BYTES_PYTHON``.

Data Mappings
-------------

The following table shows which Python types map directly to Aerospike server types.

 ======================================== =========================
  Python Type                              Server type
 ======================================== =========================
  :class:`int`                             `integer`_
  :class:`bool`                            depends on send_bool_as
  :class:`str`                             `string`_
  :class:`unicode`                         `string`_
  :class:`float`                           `double`_
  :class:`dict`                            `map`_
  :class:`aerospike.KeyOrderedDict`        `key ordered map`_
  :class:`list`                            `list`_
  :class:`bytes`                           `blob`_
  :class:`aerospike.GeoJSON`               `GeoJSON`_
  :class:`aerospike_helpers.HyperLogLog`   `HyperLogLog`_
 ======================================== =========================

For server 7.1 and higher, map keys can only be of type string, bytes, and integer.

.. note::

    :ref:`KeyOrderedDict <aerospike.KeyOrderedDict>` is a special case. Like :class:`dict`, :class:`~aerospike.KeyOrderedDict` maps to the Aerospike map data type. \
    However, the map will be sorted in key order before being sent to the server (see :ref:`aerospike_map_order`).

It is possible to nest these data types. For example a list may contain a dictionary, or a dictionary may contain a list
as a value.

.. _integer: https://aerospike.com/docs/develop/data-types/scalar/#integer
.. _string: https://aerospike.com/docs/develop/data-types/scalar/#string
.. _double: https://aerospike.com/docs/develop/data-types/scalar/#double
.. _map: https://aerospike.com/docs/develop/data-types/collections/map/
.. _key ordered map: https://aerospike.com/docs/develop/data-types/collections/ordering/#map
.. _list: https://aerospike.com/docs/develop/data-types/collections/list/
.. _blob: https://aerospike.com/docs/develop/data-types/blob
.. _GeoJSON: https://aerospike.com/docs/develop/data-types/geospatial/
.. _HyperLogLog: https://aerospike.com/docs/develop/data-types/hll
